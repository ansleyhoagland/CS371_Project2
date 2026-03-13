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

#define MAX_EVENTS 64
#define MESSAGE_SIZE 16
#define DEFAULT_CLIENT_THREADS 4
#define WINDOW_SIZE 50

typedef struct {
    int seq_num;
    char data[MESSAGE_SIZE];
} packet_t;

char *server_ip = "127.0.0.1";
int server_port = 12345;
int num_client_threads = DEFAULT_CLIENT_THREADS;
int num_requests = 1000000;

typedef struct {
    int epoll_fd;
    int socket_fd;
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

    struct timeval *start_times = malloc(sizeof(struct timeval) * num_requests);
    if (!start_times) {
        perror("malloc");
        return NULL;
    }

    int sent_idx = 0;
    int acked_idx = 0;
    int timeout_limit = 0;

    while (acked_idx < num_requests) {
        while (sent_idx < num_requests && (sent_idx - acked_idx) < WINDOW_SIZE) {
            pkt.seq_num = sent_idx;
            strncpy(pkt.data, "TASK1_PIPELINE", MESSAGE_SIZE);

            gettimeofday(&start_times[sent_idx], NULL);
            if (send(data->socket_fd, &pkt, sizeof(packet_t), 0) < 0) {
                break;
            }
            data->tx_cnt++;
            sent_idx++;
        }

        int nfds = epoll_wait(data->epoll_fd, events, MAX_EVENTS, 0);
        if (nfds > 0) {
            timeout_limit = 0;
            for (int j = 0; j < nfds; j++) {
                packet_t ack_pkt;
                while (recv(data->socket_fd, &ack_pkt, sizeof(packet_t), MSG_DONTWAIT) > 0) {
                    struct timeval end;
                    gettimeofday(&end, NULL);
                    data->rx_cnt++;
                    acked_idx++;

                    if (ack_pkt.seq_num >= 0 && ack_pkt.seq_num < num_requests) {
                        data->total_rtt += (end.tv_sec - start_times[ack_pkt.seq_num].tv_sec) * 1000000LL + (end.tv_usec - start_times[ack_pkt.seq_num].tv_usec);
                    }
                }
            }
        } else {
            if (++timeout_limit > 500000) break;
        }
    }

    free(start_times);
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

    long total_tx = 0, total_rx = 0;
    long long total_rtt = 0;
    struct timeval start_total, end_total;

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(server_port);
    inet_pton(AF_INET, server_ip, &server_addr.sin_addr);

    gettimeofday(&start_total, NULL);

    for (int i = 0; i < num_client_threads; i++) {
        int sock = socket(AF_INET, SOCK_DGRAM, 0);
        if (sock < 0) { perror("socket"); exit(1); }

        if (connect(sock, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
            perror("connect"); exit(1);
        }

        int epoll_fd = epoll_create1(0);
        struct epoll_event ev;
        ev.events = EPOLLIN;
        ev.data.fd = sock;
        epoll_ctl(epoll_fd, EPOLL_CTL_ADD, sock, &ev);

        thread_data[i].socket_fd = sock;
        thread_data[i].epoll_fd = epoll_fd;
        thread_data[i].tx_cnt = 0;
        thread_data[i].rx_cnt = 0;
        thread_data[i].total_rtt = 0;

        pthread_create(&threads[i], NULL, client_thread_func, &thread_data[i]);
    }

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

    long lost_pkt_cnt = total_tx - total_rx;

    printf("\n--- Task 1 Results ---\n");
    printf("Total Packets Sent (tx_cnt): %ld\n", total_tx);
    printf("Total Packets Received (rx_cnt): %ld\n", total_rx);
    printf("Packets Lost: %ld\n", lost_pkt_cnt);
    if (total_tx > 0) {
        printf("Packet Loss Rate: %.2f%%\n", ((double)lost_pkt_cnt / total_tx) * 100.0);
    }
    if (total_rx > 0) {
        printf("Average RTT: %lld us\n", total_rtt / total_rx);
        printf("Throughput: %.2f requests/s\n", total_rx / time_spent);
    }
}

void run_server() {
int server_fd, epoll_fd;
    struct epoll_event ev, events[MAX_EVENTS];
    struct sockaddr_in server_addr;

    server_fd = socket(AF_INET, SOCK_DGRAM, 0);

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(server_port);
    server_addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("bind"); exit(1);
    }

    epoll_fd = epoll_create1(0);
    ev.events = EPOLLIN;
    ev.data.fd = server_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev);

    printf("UDP Server listening on port %d (Task 1)...\n", server_port);

    while (1) {
        int nfds = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
        for (int i = 0; i < nfds; i++) {
            if (events[i].data.fd == server_fd) {
                packet_t pkt;
                struct sockaddr_in client_addr;
                socklen_t client_len = sizeof(client_addr);

                int n = recvfrom(server_fd, &pkt, sizeof(packet_t), 0, (struct sockaddr *)&client_addr, &client_len);

                if (n > 0) {
                    sendto(server_fd, &pkt, n, 0, (struct sockaddr *)&client_addr, client_len);
                }
            }
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
