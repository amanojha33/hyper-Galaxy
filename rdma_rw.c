
#include <assert.h>
#include <byteswap.h>
#include <endian.h>
#include <errno.h>
#include <getopt.h>
#include <infiniband/verbs.h>
#include <inttypes.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#define MAX_POLL_CQ_TIMEOUT 2000 // ms
#define MSG "This is alice, how are you?"
#define RDMAMSGR "RDMA read operation"
#define RDMAMSGW "RDMA write operation"
#define MSG_SIZE (strlen(MSG) + 1)

#if __BYTE_ORDER == __LITTLE_ENDIAN
static inline uint64_t htonll(uint64_t x) { return bswap_64(x); }
static inline uint64_t ntohll(uint64_t x) { return bswap_64(x); }
#elif __BYTE_ORDER == __BIG_ENDIAN
static inline uint64_t htonll(uint64_t x) { return x; }
static inline uint64_t ntohll(uint64_t x) { return x; }
#else
#error __BYTE_ORDER is neither __LITTLE_ENDIAN nor __BIG_ENDIAN
#endif

#define ERROR(fmt, args...)                                                    \
    { fprintf(stderr, "ERROR: %s(): " fmt, __func__, ##args); }
#define ERR_DIE(fmt, args...)                                                  \
    {                                                                          \
        ERROR(fmt, ##args);                                                    \
        exit(EXIT_FAILURE);                                                    \
    }
#define INFO(fmt, args...)                                                     \
    { printf("INFO: %s(): " fmt, __func__, ##args); }
#define WARN(fmt, args...)                                                     \
    { printf("WARN: %s(): " fmt, __func__, ##args); }

#define CHECK(expr)                                                            \
    {                                                                          \
        int rc = (expr);                                                       \
        if (rc != 0) {                                                         \
            perror(strerror(errno));                                           \
            exit(EXIT_FAILURE);                                                \
        }                                                                      \
    }

struct config_t {
    const char *dev_name; 
    char *server_name;    
    uint32_t tcp_port;    
    int ib_port;         
    int gid_idx;          
};

struct cm_con_data_t {
    uint64_t addr;   
    uint32_t rkey;   
    uint32_t qp_num; 
    uint16_t lid;    
    uint8_t gid[16]; 
} __attribute__((packed));

struct resources {
    struct ibv_device_attr device_attr; 
    struct ibv_port_attr port_attr;     
    struct cm_con_data_t remote_props;  
    struct ibv_context *ib_ctx;         
    struct ibv_pd *pd;                  
    struct ibv_cq *cq;                  
    struct ibv_qp *qp;                 
    struct ibv_mr *mr;                  
    char *buf;                        
                                        
    int sock;                           
};

struct config_t config = {.dev_name = NULL,
                          .server_name = NULL,
                          .tcp_port = 20000,
                          .ib_port = 1,
                          .gid_idx = -1};


static int sock_connect(const char *server_name, int port) {
    struct addrinfo *resolved_addr = NULL;
    struct addrinfo *iterator;
    char service[6];
    int sockfd = -1;
    int listenfd = 0;


    struct addrinfo hints = {.ai_flags = AI_PASSIVE,
                             .ai_family = AF_INET,
                             .ai_socktype = SOCK_STREAM};

    sprintf(service, "%d", port);
    CHECK(getaddrinfo(server_name, service, &hints, &resolved_addr));

    for (iterator = resolved_addr; iterator != NULL;
         iterator = iterator->ai_next) {
        sockfd = socket(iterator->ai_family, iterator->ai_socktype,
                        iterator->ai_protocol);
        assert(sockfd >= 0);

        if (server_name == NULL) {
            listenfd = sockfd;
            CHECK(bind(listenfd, iterator->ai_addr, iterator->ai_addrlen));
            CHECK(listen(listenfd, 1));
            sockfd = accept(listenfd, NULL, 0);
        } else {
            CHECK(connect(sockfd, iterator->ai_addr, iterator->ai_addrlen));
        }
    }

    return sockfd;
}

int sock_sync_data(int sockfd, int xfer_size, char *local_data,
                   char *remote_data) {
    int read_bytes = 0;
    int write_bytes = 0;

    write_bytes = write(sockfd, local_data, xfer_size);
    assert(write_bytes == xfer_size);

    read_bytes = read(sockfd, remote_data, xfer_size);
    assert(read_bytes == xfer_size);

    INFO("SYNCHRONIZED!\n\n");

    // FIXME: hard code that always returns no error
    return 0;
}

static int poll_completion(struct resources *res) {
    struct ibv_wc wc;
    unsigned long start_time_ms;
    unsigned long curr_time_ms;
    struct timeval curr_time;
    int poll_result;

   
    gettimeofday(&curr_time, NULL);
    start_time_ms = (curr_time.tv_sec * 1000) + (curr_time.tv_usec / 1000);
    do {
        poll_result = ibv_poll_cq(res->cq, 1, &wc);
        gettimeofday(&curr_time, NULL);
        curr_time_ms = (curr_time.tv_sec * 1000) + (curr_time.tv_usec / 1000);
    } while ((poll_result == 0) &&
             ((curr_time_ms - start_time_ms) < MAX_POLL_CQ_TIMEOUT));

    if (poll_result < 0) {
        ERROR("poll CQ failed\n");
        goto die;
    } else if (poll_result == 0) {
        ERROR("Completion wasn't found in the CQ after timeout\n");
        goto die;
    } else {
        INFO("Completion was found in CQ with status 0x%x\n", wc.status);
    }

    if (wc.status != IBV_WC_SUCCESS) {
        ERROR("Got bad completion with status: 0x%x, vendor syndrome: 0x%x\n",
              wc.status, wc.vendor_err);
        goto die;
    }

    // FIXME: ;)
    return 0;
die:
    exit(EXIT_FAILURE);
}


static int post_send(struct resources *res, int opcode) {
    struct ibv_send_wr sr;
    struct ibv_sge sge;
    struct ibv_send_wr *bad_wr = NULL;

    memset(&sge, 0, sizeof(sge));

    sge.addr = (uintptr_t)res->buf;
    sge.length = MSG_SIZE;
    sge.lkey = res->mr->lkey;

    memset(&sr, 0, sizeof(sr));

    sr.next = NULL;
    sr.wr_id = 0;
    sr.sg_list = &sge;

    sr.num_sge = 1;
    sr.opcode = opcode;
    sr.send_flags = IBV_SEND_SIGNALED;

    if (opcode != IBV_WR_SEND) {
        sr.wr.rdma.remote_addr = res->remote_props.addr;
        sr.wr.rdma.rkey = res->remote_props.rkey;
    }

    CHECK(ibv_post_send(res->qp, &sr, &bad_wr));

    switch (opcode) {
    case IBV_WR_SEND:
        INFO("Send request was posted\n");
        break;
    case IBV_WR_RDMA_READ:
        INFO("RDMA read request was posted\n");
        break;
    case IBV_WR_RDMA_WRITE:
        INFO("RDMA write request was posted\n");
        break;
    default:
        INFO("Unknown request was posted\n");
        break;
    }

    // FIXME: ;)
    return 0;
}

static int post_receive(struct resources *res) {
    struct ibv_recv_wr rr;
    struct ibv_sge sge;
    struct ibv_recv_wr *bad_wr;

    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)res->buf;
    sge.length = MSG_SIZE;
    sge.lkey = res->mr->lkey;

    memset(&rr, 0, sizeof(rr));

    rr.next = NULL;
    rr.wr_id = 0;
    rr.sg_list = &sge;
    rr.num_sge = 1;

    CHECK(ibv_post_recv(res->qp, &rr, &bad_wr));
    INFO("Receive request was posted\n");

    return 0;
}

static void resources_init(struct resources *res) {
    memset(res, 0, sizeof(*res));
    res->sock = -1;
}

static int resources_create(struct resources *res) {
    struct ibv_device **dev_list = NULL;
    struct ibv_qp_init_attr qp_init_attr;
    struct ibv_device *ib_dev = NULL;

    size_t size;
    int i;
    int mr_flags = 0;
    int cq_size = 0;
    int num_devices;

    if (config.server_name) {
        res->sock = sock_connect(config.server_name, config.tcp_port);
        if (res->sock < 0) {
            ERROR("Failed to establish TCP connection to server %s, port %d\n",
                  config.server_name, config.tcp_port);
            goto die;
        }
    } else {
        INFO("Waiting on port %d for TCP connection\n", config.tcp_port);
        res->sock = sock_connect(NULL, config.tcp_port);
        if (res->sock < 0) {
            ERROR("Failed to establish TCP connection with client on port %d\n",
                  config.tcp_port);
            goto die;
        }
    }

    INFO("TCP connection was established\n");
    INFO("Searching for IB devices in host\n");

    dev_list = ibv_get_device_list(&num_devices);
    assert(dev_list != NULL);

    if (num_devices == 0) {
        ERROR("Found %d device(s)\n", num_devices);
        goto die;
    }

    INFO("Found %d device(s)\n", num_devices);

    for (i = 0; i < num_devices; i++) {
        if (!config.dev_name) {
            config.dev_name = strdup(ibv_get_device_name(dev_list[i]));
            INFO("Device not specified, using first one found: %s\n",
                 config.dev_name);
        }

        if (strcmp(ibv_get_device_name(dev_list[i]), config.dev_name) == 0) {
            ib_dev = dev_list[i];
            break;
        }
    }

    if (!ib_dev) {
        ERROR("IB device %s wasn't found\n", config.dev_name);
        goto die;
    }

    res->ib_ctx = ibv_open_device(ib_dev);
    assert(res->ib_ctx != NULL);

    CHECK(ibv_query_port(res->ib_ctx, config.ib_port, &res->port_attr));

    res->pd = ibv_alloc_pd(res->ib_ctx);
    assert(res->pd != NULL);

    cq_size = 1;
    res->cq = ibv_create_cq(res->ib_ctx, cq_size, NULL, NULL, 0);
    assert(res->cq != NULL);

    size = MSG_SIZE;
    res->buf = (char *)calloc(1, size);
    assert(res->buf != NULL);

    if (!config.server_name) {
        strcpy(res->buf, MSG);
        INFO("Going to send the message: %s\n", res->buf);
    }

    mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
               IBV_ACCESS_REMOTE_WRITE;
    res->mr = ibv_reg_mr(res->pd, res->buf, size, mr_flags);
    assert(res->mr != NULL);

    INFO(
        "MR was registered with addr=%p, lkey= 0x%x, rkey= 0x%x, flags= 0x%x\n",
        res->buf, res->mr->lkey, res->mr->rkey, mr_flags);

    memset(&qp_init_attr, 0, sizeof(qp_init_attr));
    qp_init_attr.qp_type = IBV_QPT_RC;
    qp_init_attr.sq_sig_all = 1;
    qp_init_attr.send_cq = res->cq;
    qp_init_attr.recv_cq = res->cq;
    qp_init_attr.cap.max_send_wr = 1;
    qp_init_attr.cap.max_recv_wr = 1;
    qp_init_attr.cap.max_send_sge = 1;
    qp_init_attr.cap.max_recv_sge = 1;

    res->qp = ibv_create_qp(res->pd, &qp_init_attr);
    assert(res->qp != NULL);

    INFO("QP was created, QP number= 0x%x\n", res->qp->qp_num);

    // FIXME: hard code here
    return 0;
die:
    exit(EXIT_FAILURE);
}

static int modify_qp_to_init(struct ibv_qp *qp) {
    struct ibv_qp_attr attr;
    int flags;

    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = config.ib_port;
    attr.pkey_index = 0;
    attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                           IBV_ACCESS_REMOTE_WRITE;

    flags =
        IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;

    CHECK(ibv_modify_qp(qp, &attr, flags));

    INFO("Modify QP to INIT done!\n");

    // FIXME: ;)
    return 0;
}

static int modify_qp_to_rtr(struct ibv_qp *qp, uint32_t remote_qpn,
                            uint16_t dlid, uint8_t *dgid) {
    struct ibv_qp_attr attr;
    int flags;

    memset(&attr, 0, sizeof(attr));

    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = IBV_MTU_256;
    attr.dest_qp_num = remote_qpn;
    attr.rq_psn = 0;
    attr.max_dest_rd_atomic = 1;
    attr.min_rnr_timer = 0x12;
    attr.ah_attr.is_global = 0;
    attr.ah_attr.dlid = dlid;
    attr.ah_attr.sl = 0;
    attr.ah_attr.src_path_bits = 0;
    attr.ah_attr.port_num = config.ib_port;

    if (config.gid_idx >= 0) {
        attr.ah_attr.is_global = 1;
        attr.ah_attr.port_num = 1;
        memcpy(&attr.ah_attr.grh.dgid, dgid, 16);
        attr.ah_attr.grh.flow_label = 0;
        attr.ah_attr.grh.hop_limit = 1;
        attr.ah_attr.grh.sgid_index = config.gid_idx;
        attr.ah_attr.grh.traffic_class = 0;
    }

    flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
            IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;

    CHECK(ibv_modify_qp(qp, &attr, flags));

    INFO("Modify QP to RTR done!\n");

    // FIXME: ;)
    return 0;
}

static int modify_qp_to_rts(struct ibv_qp *qp) {
    struct ibv_qp_attr attr;
    int flags;

    memset(&attr, 0, sizeof(attr));

    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 0x12; // 18
    attr.retry_cnt = 6;
    attr.rnr_retry = 0;
    attr.sq_psn = 0;
    attr.max_rd_atomic = 1;

    flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
            IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;

    CHECK(ibv_modify_qp(qp, &attr, flags));

    INFO("Modify QP to RTS done!\n");

    // FIXME: ;)
    return 0;
}

static int connect_qp(struct resources *res) {
    struct cm_con_data_t local_con_data;
    struct cm_con_data_t remote_con_data;
    struct cm_con_data_t tmp_con_data;
    char temp_char;
    union ibv_gid my_gid;

    memset(&my_gid, 0, sizeof(my_gid));

    if (config.gid_idx >= 0) {
        CHECK(ibv_query_gid(res->ib_ctx, config.ib_port, config.gid_idx,
                            &my_gid));
    }

    local_con_data.addr = htonll((uintptr_t)res->buf);
    local_con_data.rkey = htonl(res->mr->rkey);
    local_con_data.qp_num = htonl(res->qp->qp_num);
    local_con_data.lid = htons(res->port_attr.lid);
    memcpy(local_con_data.gid, &my_gid, 16);

    INFO("\n Local LID      = 0x%x\n", res->port_attr.lid);

    sock_sync_data(res->sock, sizeof(struct cm_con_data_t),
                   (char *)&local_con_data, (char *)&tmp_con_data);

    remote_con_data.addr = ntohll(tmp_con_data.addr);
    remote_con_data.rkey = ntohl(tmp_con_data.rkey);
    remote_con_data.qp_num = ntohl(tmp_con_data.qp_num);
    remote_con_data.lid = ntohs(tmp_con_data.lid);
    memcpy(remote_con_data.gid, tmp_con_data.gid, 16);

    res->remote_props = remote_con_data;

    INFO("Remote address = 0x%" PRIx64 "\n", remote_con_data.addr);
    INFO("Remote rkey = 0x%x\n", remote_con_data.rkey);
    INFO("Remote QP number = 0x%x\n", remote_con_data.qp_num);
    INFO("Remote LID = 0x%x\n", remote_con_data.lid);

    if (config.gid_idx >= 0) {
        uint8_t *p = remote_con_data.gid;
        int i;
        printf("Remote GID = ");
        for (i = 0; i < 15; i++)
            printf("%02x:", p[i]);
        printf("%02x\n", p[15]);
    }

    modify_qp_to_init(res->qp);

    if (config.server_name) {
        post_receive(res);
    }

    modify_qp_to_rtr(res->qp, remote_con_data.qp_num, remote_con_data.lid,
                     remote_con_data.gid);

    if (config.server_name)
        modify_qp_to_rts(res->qp);

    sock_sync_data(res->sock, 1, "Q", &temp_char);

    // FIXME: ;)
    return 0;
}


static int resources_destroy(struct resources *res) {
    ibv_destroy_qp(res->qp);
    ibv_dereg_mr(res->mr);
    free(res->buf);
    ibv_destroy_cq(res->cq);
    ibv_dealloc_pd(res->pd);
    ibv_close_device(res->ib_ctx);
    close(res->sock);

    // FIXME: ;)
    return 0;
}

static void print_config(void) {
    {
        INFO("Device name:          %s\n", config.dev_name);
        INFO("IB port:              %u\n", config.ib_port);
    }
    if (config.server_name) {
        INFO("IP:                   %s\n", config.server_name);
    }
    { INFO("TCP port:             %u\n", config.tcp_port); }
    if (config.gid_idx >= 0) {
        INFO("GID index:            %u\n", config.gid_idx);
    }
}

static void print_usage(const char *progname) {
    printf("Usage:\n");
    printf("%s          start a server and wait for connection\n", progname);
    printf("%s <host>   connect to server at <host>\n\n", progname);
    printf("Options:\n");
    printf("-p, --port <port>           listen on / connect to port <port> "
           "(default 20000)\n");
    printf("-d, --ib-dev <dev>          use IB device <dev> (default first "
           "device found)\n");
    printf("-i, --ib-port <port>        use port <port> of IB device (default "
           "1)\n");
    printf("-g, --gid_idx <gid index>   gid index to be used in GRH (default "
           "not used)\n");
    printf("-h, --help                  this message\n");
}


int main(int argc, char *argv[]) {
    struct resources res;
    char temp_char;

    while (1) {
        int c;

        static struct option long_options[] = {
            {"port", required_argument, 0, 'p'},
            {"ib-dev", required_argument, 0, 'd'},
            {"ib-port", required_argument, 0, 'i'},
            {"gid-idx", required_argument, 0, 'g'},
            {"help", no_argument, 0, 'h'},
            {NULL, 0, 0, 0}};

        c = getopt_long(argc, argv, "p:d:i:g:h", long_options, NULL);
        if (c == -1)
            break;

        switch (c) {
        case 'p':
            config.tcp_port = strtoul(optarg, NULL, 0);
            break;
        case 'd':
            config.dev_name = strdup(optarg);
            break;
        case 'i':
            config.ib_port = strtoul(optarg, NULL, 0);
            if (config.ib_port < 0) {
                print_usage(argv[0]);
                exit(EXIT_FAILURE);
            }
            break;
        case 'g':
            config.gid_idx = strtoul(optarg, NULL, 0);
            if (config.gid_idx < 0) {
                print_usage(argv[0]);
                exit(EXIT_FAILURE);
            }
            break;
        case 'h':
        default:
            print_usage(argv[0]);
            exit(EXIT_FAILURE);
        }
    }

    if (optind == argc - 1) {
        config.server_name = argv[optind];
    } else if (optind < argc) {
        print_usage(argv[0]);
        exit(EXIT_FAILURE);
    }

    print_config();
    resources_init(&res);
    resources_create(&res);
    connect_qp(&res);

    // if (!config.server_name)
    //     post_send(&res, IBV_WR_SEND);

    // poll_completion(&res);

    // if (config.server_name) {
    //     INFO("Message is: %s\n", res.buf);
    // } else 
    //     strcpy(res.buf, RDMAMSGR);
    // }

    sock_sync_data(res.sock, 1, "R",
                   &temp_char); 

    if (config.server_name) {
        post_send(&res, IBV_WR_RDMA_READ);
        poll_completion(&res);

        INFO("Contents of server's buffer: %s\n", res.buf);
        strcpy(res.buf, RDMAMSGW);
        INFO("Now replacing it with: %s\n", res.buf);

        post_send(&res, IBV_WR_RDMA_WRITE);
        poll_completion(&res);
    }

    sock_sync_data(res.sock, 1, "W", &temp_char);
    if (!config.server_name)
        INFO("Contents of server buffer: %s\n", res.buf);

    // whatever
    resources_destroy(&res);

    return 0;
}
