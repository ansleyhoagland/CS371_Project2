/*
# Copyright 2025 University of Kentucky
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# SPDX-License-Identifier: Apache-2.0
*/

/*
Please specify the group members here

# Student #1: Ansley Hoagland
# Student #2: N/A
# Student #3: N/A

*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <pthread.h>
#include <stdbool.h>

#define MAX_EVENTS 64
#define MESSAGE_SIZE 16
#define DEFAULT_CLIENT_THREADS 4
#define WINDOW_SIZE 50
#define TIMEOUT_MS 10

typedef struct {
    int seq_num;
    char data[MESSAGE_SIZE];
} packet_t;

char *server_ip = "127.0.0.1";
int server_port = 12345;
int num_client_threads = DEFAULT_CLIENT_THREADS;
int num_requests = 1000000;

/*
 * This structure is used to store per-thread data in the client
 */
typedef struct {
    int epoll_fd;        /* File descriptor for the epoll instance, used for monitoring events on the socket. */
    int socket_fd;       /* File descriptor for the client socket connected to the server. */
    int thread_requests;
    long tx_cnt;
    long rx_cnt;
    long long total_rtt;
} client_thread_data_t;

/*
 * This function runs in a separate client thread to handle communication with the server
*/
void *client_thread_func(void *arg) {
    client_thread_data_t *data = (client_thread_data_t *)arg;
    struct epoll_event events[MAX_EVENTS];
    packet_t pkt;

    int thread_reqs = data->thread_requests;
    struct timeval *start_times = malloc(sizeof(struct timeval) * thread_reqs);
    bool *is_sent = calloc(thread_reqs, sizeof(bool));
    struct timeval end;

    int sent_idx = 0;
    int acked_idx = 0;

    while (acked_idx < thread_reqs) {
        while (sent_idx < thread_reqs && (sent_idx - acked_idx) < WINDOW_SIZE) {
            pkt.seq_num = sent_idx;
            strncpy(pkt.data, "TASK2_GBN", MESSAGE_SIZE);

            gettimeofday(&start_times[sent_idx], NULL);
            send(data->socket_fd, &pkt, sizeof(packet_t), 0);

            data->tx_cnt++;
            sent_idx++;
        }

        int nfds = epoll_wait(data->epoll_fd, events, MAX_EVENTS, TIMEOUT_MS);
        if (nfds > 0) {
            for (int j = 0; j < nfds; j++) {
                packet_t ack_pkt;

                if (recv(data->socket_fd, &ack_pkt, sizeof(packet_t), 0) > 0) {
                    if (ack_pkt.seq_num >= acked_idx && ack_pkt.seq_num < thread_reqs) {
                        gettimeofday(&end, NULL);

                        for (int i = acked_idx; i <= ack_pkt.seq_num; i++) {
                            data->rx_cnt++;
                            data->total_rtt += (end.tv_sec - start_times[i].tv_sec) * 1000000LL + (end.tv_usec - start_times[i].tv_usec);
                        }
                        acked_idx = ack_pkt.seq_num + 1;
                    }
                }
            }
        } else {
            sent_idx = acked_idx;
        }
    }

    free(start_times);
    free(is_sent);
    return NULL;
}

/*
 * This function orchestrates multiple client threads to send requests to a server,
 * collect performance data of each threads, and compute aggregated metrics of all threads.
 */
void run_client() {
    pthread_t threads[num_client_threads];
    client_thread_data_t thread_data[num_client_threads];
    struct sockaddr_in server_addr;

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(server_port);
    inet_pton(AF_INET, server_ip, &server_addr.sin_addr);

    struct timeval start_total, end_total;
    gettimeofday(&start_total, NULL);

    int reqs_per_thread = num_requests / num_client_threads;

    for (int i = 0; i < num_client_threads; i++) {
        int sock = socket(AF_INET, SOCK_DGRAM, 0);
        connect(sock, (struct sockaddr *)&server_addr, sizeof(server_addr));

        int epoll_fd = epoll_create1(0);
        struct epoll_event ev = {.events = EPOLLIN, .data.fd = sock};
        epoll_ctl(epoll_fd, EPOLL_CTL_ADD, sock, &ev);

        thread_data[i].socket_fd = sock;
        thread_data[i].epoll_fd = epoll_fd;
        thread_data[i].thread_requests = reqs_per_thread;
        thread_data[i].tx_cnt = 0;
        thread_data[i].rx_cnt = 0;
        thread_data[i].total_rtt = 0;

        pthread_create(&threads[i], NULL, client_thread_func, &thread_data[i]);
    }

    long total_tx = 0, total_rx = 0;
    long long total_rtt = 0;

    for (int i = 0; i < num_client_threads; i++) {
        pthread_join(threads[i], NULL);
        total_tx += thread_data[i].tx_cnt;
        total_rx += thread_data[i].rx_cnt;
        total_rtt += thread_data[i].total_rtt;
        close(thread_data[i].socket_fd);
        close(thread_data[i].epoll_fd);
    }

    gettimeofday(&end_total, NULL);
    double time_spent = (end_total.tv_sec - start_total.tv_sec) + (end_total.tv_usec - start_total.tv_usec) / 1000000.0;

    printf("\n--- Task 2 Results (Reliable) ---\n");
    printf("Total Packets Sent (including retrans): %ld\n", total_tx);
    printf("Total Unique Packets Received: %ld\n", total_rx);
    printf("Retransmissions: %ld\n", total_tx - total_rx);
    if (total_rx > 0) {
        printf("Average RTT: %lld us\n", total_rtt / total_rx);
        printf("Throughput: %.2f requests/s\n", (double)total_rx / time_spent);
    }
}

void run_server() {
    int server_fd = socket(AF_INET, SOCK_DGRAM, 0);
    struct sockaddr_in server_addr = {
        .sin_family = AF_INET,
        .sin_port = htons(server_port),
        .sin_addr.s_addr = INADDR_ANY
    };

    bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr));
    printf("UDP Echo Server (Task 2) listening on port %d...\n", server_port);

    while (1) {
        packet_t pkt;
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int n = recvfrom(server_fd, &pkt, sizeof(packet_t), 0, (struct sockaddr *)&client_addr, &client_len);

        if (n > 0) {
            sendto(server_fd, &pkt, sizeof(packet_t), 0, (struct sockaddr *)&client_addr, client_len);
        }
    }
}

int main(int argc, char *argv[]) {
    if (argc > 1 && strcmp(argv[1], "server") == 0) {
        if (argc > 2) server_ip = argv[2];
        if (argc > 3) server_port = atoi(argv[3]);

        run_server();
    } else if (argc > 1 && strcmp(argv[1], "client") == 0) {
        if (argc > 2) server_ip = argv[2];
        if (argc > 3) server_port = atoi(argv[3]);
        if (argc > 4) num_client_threads = atoi(argv[4]);
        if (argc > 5) num_requests = atoi(argv[5]);
        run_client();
    } else {
        printf("Usage: %s <server|client> [server_ip server_port num_client_threads num_requests]\n", argv[0]);
    }

    return 0;
}
