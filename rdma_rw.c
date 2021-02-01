
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

// structure of test parameters
struct config_t {
    const char *dev_name; // IB device name
    char *server_name;    // server hostname
    uint32_t tcp_port;    // server TCP port
    int ib_port;          // local IB port to work with
    int gid_idx;          // GID index to use
};

// structure to exchange data which is needed to connect the QPs
struct cm_con_data_t {
    uint64_t addr;   // buffer address
    uint32_t rkey;   // remote key
    uint32_t qp_num; // QP number
    uint16_t lid;    // LID of the IB port
    uint8_t gid[16]; // GID
} __attribute__((packed));

// structure of system resources
struct resources {
    struct ibv_device_attr device_attr; // device attributes
    struct ibv_port_attr port_attr;     // IB port attributes
    struct cm_con_data_t remote_props;  // values to connect to remote side
    struct ibv_context *ib_ctx;         // device handle
    struct ibv_pd *pd;                  // PD handle
    struct ibv_cq *cq;                  // CQ handle
    struct ibv_qp *qp;                  // QP handle
    struct ibv_mr *mr;                  // MR handle for buf
    char *buf;                          // memory buffer pointer, used for
                                        // RDMA send ops
    int sock;                           // TCP socket file descriptor
};

struct config_t config = {.dev_name = NULL,
                          .server_name = NULL,
                          .tcp_port = 20000,
                          .ib_port = 1,
                          .gid_idx = -1};

// \begin socket operation
//
// For simplicity, the example program uses TCP sockets to exchange control
// information. If a TCP/IP stack/connection is not available, connection
// manager (CM) may be used to pass this information. Use of CM is beyond the
// scope of this example.

// Connect a socket. If servername is specified a client connection will be
// initiated to the indicated server and port. Otherwise listen on the indicated
// port for an incoming connection.
static int sock_connect(const char *server_name, int port) {
    struct addrinfo *resolved_addr = NULL;
    struct addrinfo *iterator;
    char service[6];
    int sockfd = -1;
    int listenfd = 0;

    // @man getaddrinfo:
    //  struct addrinfo {
    //      int             ai_flags;
    //      int             ai_family;
    //      int             ai_socktype;
    //      int             ai_protocol;
    //      socklen_t       ai_addrlen;
    //      struct sockaddr *ai_addr;
    //      char            *ai_canonname;
    //      struct addrinfo *ai_next;
    //  }
    struct addrinfo hints = {.ai_flags = AI_PASSIVE,
                             .ai_family = AF_INET,
                             .ai_socktype = SOCK_STREAM};

    // resolve DNS address, user sockfd as temp storage
    sprintf(service, "%d", port);
    CHECK(getaddrinfo(server_name, service, &hints, &resolved_addr));

    for (iterator = resolved_addr; iterator != NULL;
         iterator = iterator->ai_next) {
        sockfd = socket(iterator->ai_family, iterator->ai_socktype,
                        iterator->ai_protocol);
        assert(sockfd >= 0);

        if (server_name == NULL) {
            // Server mode: setup listening socket and accept a connection
            listenfd = sockfd;
            CHECK(bind(listenfd, iterator->ai_addr, iterator->ai_addrlen));
            CHECK(listen(listenfd, 1));
            sockfd = accept(listenfd, NULL, 0);
        } else {
            // Client mode: initial connection to remote
            CHECK(connect(sockfd, iterator->ai_addr, iterator->ai_addrlen));
        }
    }

    return sockfd;
}

// Sync data across a socket. The indicated local data will be sent to the
// remote. It will then wait for the remote to send its data back. It is
// assumned that the two sides are in sync and call this function in the proper
// order. Chaos will ensure if they are not. Also note this is a blocking
// function and will wait for the full data to be received from the remote.
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
// \end socket operation

// Poll the CQ for a single event. This function will continue to poll the queue
// until MAX_POLL_TIMEOUT ms have passed.
static int poll_completion(struct resources *res) {
    struct ibv_wc wc;
    unsigned long start_time_ms;
    unsigned long curr_time_ms;
    struct timeval curr_time;
    int poll_result;

    // poll the completion for a while before giving up of doing it
    gettimeofday(&curr_time, NULL);
    start_time_ms = (curr_time.tv_sec * 1000) + (curr_time.tv_usec / 1000);
    do {
        poll_result = ibv_poll_cq(res->cq, 1, &wc);
        gettimeofday(&curr_time, NULL);
        curr_time_ms = (curr_time.tv_sec * 1000) + (curr_time.tv_usec / 1000);
    } while ((poll_result == 0) &&
             ((curr_time_ms - start_time_ms) < MAX_POLL_CQ_TIMEOUT));

    if (poll_result < 0) {
        // poll CQ failed
        ERROR("poll CQ failed\n");
        goto die;
    } else if (poll_result == 0) {
        ERROR("Completion wasn't found in the CQ after timeout\n");
        goto die;
    } else {
        // CQE found
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

// This function will create and post a send work request.
static int post_send(struct resources *res, int opcode) {
    struct ibv_send_wr sr;
    struct ibv_sge sge;
    struct ibv_send_wr *bad_wr = NULL;

    // prepare the scatter / gather entry
    memset(&sge, 0, sizeof(sge));

    sge.addr = (uintptr_t)res->buf;
    sge.length = MSG_SIZE;
    sge.lkey = res->mr->lkey;

    // prepare the send work request
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

    // there is a receive request in the responder side, so we won't get any
    // into RNR flow
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

    // prepare the scatter / gather entry
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)res->buf;
    sge.length = MSG_SIZE;
    sge.lkey = res->mr->lkey;

    // prepare the receive work request
    memset(&rr, 0, sizeof(rr));

    rr.next = NULL;
    rr.wr_id = 0;
    rr.sg_list = &sge;
    rr.num_sge = 1;

    // post the receive request to the RQ
    CHECK(ibv_post_recv(res->qp, &rr, &bad_wr));
    INFO("Receive request was posted\n");

    return 0;
}

// Res is initialized to default values
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
        // @client
        res->sock = sock_connect(config.server_name, config.tcp_port);
        if (res->sock < 0) {
            ERROR("Failed to establish TCP connection to server %s, port %d\n",
                  config.server_name, config.tcp_port);
            goto die;
        }
    } else {
        // @server
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

    // \begin acquire a specific device
    // get device names in the system
    dev_list = ibv_get_device_list(&num_devices);
    assert(dev_list != NULL);

    if (num_devices == 0) {
        ERROR("Found %d device(s)\n", num_devices);
        goto die;
    }

    INFO("Found %d device(s)\n", num_devices);

    // search for the specific device we want to work with
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

    // device wasn't found in the host
    if (!ib_dev) {
        ERROR("IB device %s wasn't found\n", config.dev_name);
        goto die;
    }

    // get device handle
    res->ib_ctx = ibv_open_device(ib_dev);
    assert(res->ib_ctx != NULL);
    // \end acquire a specific device

    // query port properties
    CHECK(ibv_query_port(res->ib_ctx, config.ib_port, &res->port_attr));

    // PD
    res->pd = ibv_alloc_pd(res->ib_ctx);
    assert(res->pd != NULL);

    // a CQ with one entry
    cq_size = 1;
    res->cq = ibv_create_cq(res->ib_ctx, cq_size, NULL, NULL, 0);
    assert(res->cq != NULL);

    // a buffer to hold the data
    size = MSG_SIZE;
    res->buf = (char *)calloc(1, size);
    assert(res->buf != NULL);

    // only in the server side put the message in the memory buffer
    if (!config.server_name) {
        strcpy(res->buf, MSG);
        INFO("Going to send the message: %s\n", res->buf);
    }

    // register the memory buffer
    mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
               IBV_ACCESS_REMOTE_WRITE;
    res->mr = ibv_reg_mr(res->pd, res->buf, size, mr_flags);
    assert(res->mr != NULL);

    INFO(
        "MR was registered with addr=%p, lkey= 0x%x, rkey= 0x%x, flags= 0x%x\n",
        res->buf, res->mr->lkey, res->mr->rkey, mr_flags);

    // \begin create the QP
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
    // \end create the QP

    // FIXME: hard code here
    return 0;
die:
    exit(EXIT_FAILURE);
}

// Transition a QP from the RESET to INIT state
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

// Transition a QP from the INIT to RTR state, using the specified QP number
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

// Transition a QP from the RTR to RTS state
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

// Connect the QP, then transition the server side to RTR, sender side to RTS.
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

    // \begin exchange required info like buffer (addr & rkey) / qp_num / lid,
    // etc. exchange using TCP sockets info required to connect QPs
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

    // save the remote side attributes, we will need it for the post SR
    res->remote_props = remote_con_data;
    // \end exchange required info

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

    // modify the QP to init
    modify_qp_to_init(res->qp);

    // let the client post RR to be prepared for incoming messages
    if (config.server_name) {
        post_receive(res);
    }
    // modify the QP to RTR
    modify_qp_to_rtr(res->qp, remote_con_data.qp_num, remote_con_data.lid,
                     remote_con_data.gid);

    // modify QP state to RTS
    modify_qp_to_rts(res->qp);

    // sync to make sure that both sides are in states that they can connect to
    // prevent packet lose
    sock_sync_data(res->sock, 1, "Q", &temp_char);

    // FIXME: ;)
    return 0;
}

// Cleanup and deallocate all resources used
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

// Concerned data structures and APIs:
//
// Establish a connection between endpoints:
//
// struct ibv_device {
//     struct _ibv_device_ops   _ops;
//     enum ibv_node_type       node_type;
//     enum ibv_transport_type  transport_type;
//     // Name of underlying kernel IB device, e.g methca0
//     char                     name[IBV_SYSFS_NAME_MAX];
//     // Name of uverbs device, e.g. uverbs0
//     char                     dev_name[IBV_SYSFS_NAME_MAX];
//     // Path to infiniband_verbs class device in sysfs
//     char                     dev_path[IBV_SYSFS_PATH_MAX];
//     // Path to infiniband class device in sysfs
//     char                     ibdev_path[IBV_SYSFS_PATH_MAX];
// };
// struct ibv_device **ibv_get_device_list(int *num_devices);
// const char *ibv_get_device_name(struct ibv_device *device);
//
// struct ibv_context {
//     struct ibv_device        *device;
//     struct ibv_context_ops   ops;
//     int                      cmd_fd;
//     int                      async_fd;
//     int                      num_com_vector;
//     pthread_mutex_t          mutex;
//     void                     *abi_compact;
// };
// struct ibv_context *ibv_open_device(struct ibv_device *device);
//
// struct ibv_port_attr {
//     enum ibv_port_state state;           // Logical port state
//     enum ibv_mtu        max_mtu;         // Max MTU supported by port
//     enum ibv_mtu        active_mtu;      // Actual MTU
//     int                 gid_tbl_len;     // Length of source GID table
//     uint32_t            port_cap_flags;  // Port capabilities
//     uint32_t            max_msg_sz;      // Maximum message size
//     uint32_t            bad_pkey_cntr;   // Bad P_Key counter
//     uint32_t            qkey_viol_cntr;  // Q_Key violation counter
//     uint16_t            pkey_tbl_len;    // Length of partition table
//     uint16_t            lid;             // Base port LID
//     uint16_t            sm_lid;          // SM LID
//     uint8_t             lmc;             // LMC of LID
//     uint8_t             max_vl_num;      // Maximum number of VLs
//     uint8_t             sm_sl;           // SM service level
//     uint8_t             subnet_timeout;  // Subnet propagation delay
//     uint8_t             init_type_reply; // Type of initialization performed
//                                          // by SM
//     uint8_t             active_width;    // Currently active link width
//     uint8_t             active_speed;    // Currently active link speed
//     uint8_t             phys_state;      // Physical port state
//     uint8_t             link_layer;      // link layer protocol of the port
//
// };
// int ibv_query_port(struct ibv_context *context, uint8_t port_num,
//                    struct ibv_port_attr *port_attr);
//
// struct ibv_pd {
//     struct ibv_mr *mr;
//     uint64_t      addr;
//     uint64_t      length;
//     unsigned int  mw_access_flags;
// };
// struct ibv_pd *ibv_alloc_pd(struct ibv_context *context);
//
// struct ibv_cq {
//     struct ibv_context       *context;
//     struct ibv_comp_channel  *channel;
//     void                     *cq_context;
//     uint32_t                 handle;
//     int                      cqe;
//     pthread_mutex_t          mutex;
//     pthread_cond_t           cond;
//     uint32_t                 comp_events_completed;
//     uint32_t                 async_events_completed;
// };
// struct ibv_cq *ibv_create_cq(struct ibv_context *context,
//                              int cqe,
//                              void *cq_context,
//                              struct ibv_com_channel *channel,
//                              int comp_vector);
//
// struct ibv_qp_init_attr {
//     void                 *qp_context;
//     struct ibv_cq        *send_cq;
//     struct ibv_cq        *recv_cq;
//     struct ibv_srq       *srq;
//     struct ibv_qp_cap    cap;
//     enum ibv_qp_type     qp_type;
//     int                  sq_sig_all;
// };
// struct ibv_qp *ibv_create_qp(struct ibv_pd *pd,
//                              struct ibv_qp_init_attr *qp_int_attr);
//
// Deliver data:
//
// struct ibv_mr *ibv_reg_mr(struct ibv_pd *pd, void *addr,
//                           size_t length, int access);
// int ibv_post_send(struct ibv_qp *qp, struct ibv_send_wr *wr,
//                   struct ibv_send_wr **bad_wr);
// int ibv_post_recv(struct ibv_qp *qp, struct ibv_recv_wr *wr,
//                   struct ibv_recv_wr **bad_wr);
//
// struct {
//     uint64_t             wr_id;
//     enum ibv_wc_status   status;
//     enum ibv_wc_opcode   opcode;
//     uint32_t             vendor_err;
//     uint32_t             byte_len;
//     // When (wc_flags & IBV_WC_WITH_IMM): Immediate data in network byte
//     // order.
//     // When (wc_flags & IBV_WC_WITH_INV): Stores the invalidated rkey.
//     union {
//         __be32   imm_data;
//         uint32_t invalidated_rkey;
//     };
//     uint32_t             qp_num;
//     uint32_t             src_qp;
//     unsigned int         wc_flags;
//     uint16_t             pkey_index;
//     uint16_t             slid;
//     uint8_t              sl;
//     uint8_t              dlid_path_bits;
// };
// int ibv_poll_cq(struct ibv_cq *cq, int num_entries,
//                 struct ibv_wc *wc);

// This function creates and allocates all necessary system resources. These are
// stored in res.
int main(int argc, char *argv[]) {
    struct resources res;
    char temp_char;

    // \begin parse command line parameters
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

    // parse the last parameter (if exists) as the server name
    if (optind == argc - 1) {
        config.server_name = argv[optind];
    } else if (optind < argc) {
        print_usage(argv[0]);
        exit(EXIT_FAILURE);
    }
    // \ned parse command line parameters

    print_config();

    // init all the resources, so cleanup will be easy
    resources_init(&res);

    // create resources before using them
    resources_create(&res);

    // connect the QPs
    connect_qp(&res);

    // let server post the sr
    if (!config.server_name)
        post_send(&res, IBV_WR_SEND);

    // in both sides we expect to get a completion
    // @server: there's a send completion
    // @client: there's a recv completion
    poll_completion(&res);

    // after polling the completion we have the message in the client buffer too
    if (config.server_name) {
        INFO("Message is: %s\n", res.buf);
    } else {
        // setup server buffer with read message
        strcpy(res.buf, RDMAMSGR);
    }

    // sync so we are sure server side has data ready before client tries to
    // read it
    sock_sync_data(res.sock, 1, "R",
                   &temp_char); // just send a dummy char back and forth

    // Now the client performs an RDMA read and then write on server. Note that
    // the server has no idea these events have occured.
    if (config.server_name) {
        // first we read contents of server's buffer
        post_send(&res, IBV_WR_RDMA_READ);
        poll_completion(&res);

        INFO("Contents of server's buffer: %s\n", res.buf);

        // now we replace what's in the server's buffer
        strcpy(res.buf, RDMAMSGW);
        INFO("Now replacing it with: %s\n", res.buf);

        post_send(&res, IBV_WR_RDMA_WRITE);
        poll_completion(&res);
    }

    // sync so server will know that client is done mucking with its memory
    sock_sync_data(res.sock, 1, "W", &temp_char);
    if (!config.server_name)
        INFO("Contents of server buffer: %s\n", res.buf);

    // whatever
    resources_destroy(&res);

    return 0;
}
