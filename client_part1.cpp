#include <bits/stdc++.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#include <thread>
#include <mutex>

#pragma comment(lib, ws2_32)
using namespace std;

int N_CLIENTS;
const char* SERVER_IP = "192.168.1.3";
mutex mu;

struct FileChunkPacket{
    int index;
    char data[1024];
    int valid_bytes;
};

/* 
    UDP sendto return values: 1024 or -1
    in -1, cases:
        WSAEMSGSIZE: datagram too big
        WSAEWOULDBLOCK: outbound buffer full
        WSAEHOSTUNREACH: host unreachable - local error, windows cant find a path to get to that socket
        WSAEINVAL: invalid send arguments
        WSAENOTSOCK: socket already closed/dead
        WSAENETUNREACH: no internet connection

    UDP recvfrom return values: 1024, 0, -1
    0: empty datagram, no concept of graceful disconnect in udp, a closesocket() is only for OS cleanup, not to tell
    the other side the connection is over
    -1:
        WSAETIMEDOUT: timeout
        WSAECONNRESET: when you used sentto on this socket earlier, but the receiver's socket was closed so the router
        sent an ICMP message to the next recvfrom on that same socket - happens at the other side, your packet reached
        there successfully but that device realised nobody is listening on that port or something else like this 
        WSAEINTR: socket closed by parent
        WSAEMSGSIZE: the datagram sent to you is too big
        WSAEWOULDBLOCK: if the socket is non-blocking and there is nothing on it right now
        WSAENOTSOCK or WSAEINVAL: socket invalid.

    TCP recv return values: 1024, 0, -1
    0: graceful disconnect, the other side has called closesocket() nicely, or maybe just  an empty datagram
    -1 cases:
        WSAECONNRESET: server violently crashed
        WSAETIMEDOUT: timeout
        WSAEINTR: socket already closed (by parent thread maybe)
        WSAECONNABORTED: connection closed on sender's machine itself (wifi disconnect etc)
        WSAENOTCONN: socket not connected yet by you 

    TCP send return values: 1024, -1
    -1/SOCKET_ERROR:
        WSAECONNRESET: the other device violently crashed
        WSAECONNABORTED: your machine's wifi disconnect
        WSAENOBUFS: your buffer isf ull
        WSAEWOULDBLOCK: non blocking buffer full
        WSAENOTCONN: socket not yet active (not connected)
        WSAEINTR: socket closed by parent thread
        
    INVALID_SOCKET is on accept call
*/

SOCKET acceptWithTimeout(SOCKET listening_socket, int timeout_sec) {
    fd_set readfds;
    FD_ZERO(&readfds);
    FD_SET(listening_socket, &readfds);

    timeval timeout;
    timeout.tv_sec = timeout_sec;
    timeout.tv_usec = 0;

    int activity = select(0, &readfds, NULL, NULL, &timeout);
    
    if (activity <= 0) {
        return INVALID_SOCKET; // Timeout or error
    }
    
    return accept(listening_socket, NULL, NULL);
}

void printErrorMessage(int bytes_received){
    if(bytes_received==0){
        cout<<"Server closed the connection."<<endl;
    }
    if(bytes_received==-1){
        cout<<WSAGetLastError()<<endl;
    }
}

void sister_thread(SOCKET sister_udp_listening_socket, string filename, bool *chunk_tracker, mutex& mu){
    while(true){
        char buf[1024] = {0};
        int bytes_received = recvfrom(sister_udp_listening_socket, buf, sizeof(buf)-1, 0, NULL, NULL);
        if(bytes_received < 0){
            printErrorMessage(bytes_received);
            return;
        }

        if(!strcmp(buf, "SHUTDOWN")){
            return;
        }

        int index;
        int reply_port;
        sscanf(buf, "CHUNK: %d REPLY_PORT: %d", &index, &reply_port);

        mu.lock();
        bool found = chunk_tracker[index];
        mu.unlock();
        if(found){
            FileChunkPacket packet;
            packet.index = index;
            memset(packet.data, 0, 1024);
            mu.lock();
            ifstream infile(filename, ios::binary);
            infile.seekg(index*1024ULL);
            infile.read(packet.data, 1024);
            packet.valid_bytes = infile.gcount();
            infile.close();
            mu.unlock();

            sockaddr_in server_addr{};
            server_addr.sin_family = AF_INET;
            server_addr.sin_port = htons(reply_port);
            server_addr.sin_addr.s_addr = inet_addr(SERVER_IP);

            SOCKET broadcast_reply_socket = socket(AF_INET, SOCK_STREAM, 0);
            int connection_result = connect(broadcast_reply_socket, (sockaddr*)&server_addr, sizeof(server_addr));
            if(connection_result==SOCKET_ERROR){
                closesocket(broadcast_reply_socket);
                continue;
            }

            int sent_bytes = 0;
            while(sent_bytes < sizeof(packet)){
                int sent = send(broadcast_reply_socket, ((char*)&packet)+sent_bytes, sizeof(packet)-sent_bytes, 0);
                if(sent <= 0) break;
                sent_bytes += sent;
            }
            closesocket(broadcast_reply_socket);
        }
        else{
            continue;
        }
    }
}

void worker(int client_id){
    mutex mu;
    string filename = "client_" + std::to_string(client_id) + "_output.txt";
    ofstream create_file(filename, ios::binary);
    create_file.close();
    cout<<"Created client with id: "<<client_id<<endl;

    int total_chunks;
    SOCKET main_request_socket = socket(AF_INET, SOCK_DGRAM, 0); // sends the hello
    DWORD main_request_socket_timeout = 60000;
    setsockopt(main_request_socket, SOL_SOCKET, SO_RCVTIMEO, (char*)&main_request_socket_timeout, sizeof(main_request_socket_timeout));
    SOCKET initial_tcp_socket = socket(AF_INET, SOCK_STREAM, 0);
    DWORD initial_tcp_socket_timeout = 2000;
    setsockopt(initial_tcp_socket, SOL_SOCKET, SO_RCVTIMEO, (char*)&initial_tcp_socket_timeout, sizeof(initial_tcp_socket_timeout));
    SOCKET sister_udp_socket = socket(AF_INET, SOCK_DGRAM, 0);
    DWORD sister_udp_timeout = 180000; // should be 60000
    // setsockopt(sister_udp_socket, SOL_SOCKET, SO_RCVTIMEO, (char*)&sister_udp_timeout, sizeof(sister_udp_timeout));
    int initial_tcp_port;
    int sister_udp_port;

    sockaddr_in server_listening_addr{};
    server_listening_addr.sin_family = AF_INET;
    server_listening_addr.sin_port = htons(8080);
    server_listening_addr.sin_addr.s_addr = inet_addr(SERVER_IP);

    sockaddr_in initial_tcp_addr{}; // to be binded
    sockaddr_in sister_udp_addr{}; // to be binded

    initial_tcp_addr.sin_family = AF_INET;
    initial_tcp_addr.sin_addr.s_addr = INADDR_ANY;
    initial_tcp_addr.sin_port = htons(0);
    bind(initial_tcp_socket, (sockaddr*)&initial_tcp_addr, sizeof(initial_tcp_addr));
    socklen_t socklen = sizeof(initial_tcp_addr);
    getsockname(initial_tcp_socket, (sockaddr*)&initial_tcp_addr, &socklen);
    initial_tcp_port = ntohs(initial_tcp_addr.sin_port);

    sister_udp_addr.sin_family = AF_INET;
    sister_udp_addr.sin_port = htons(0);
    sister_udp_addr.sin_addr.s_addr = INADDR_ANY;
    bind(sister_udp_socket, (sockaddr*)&sister_udp_addr, sizeof(sister_udp_addr));
    socklen = sizeof(sister_udp_addr);
    getsockname(sister_udp_socket, (sockaddr*)&sister_udp_addr, &socklen);
    sister_udp_port = ntohs(sister_udp_addr.sin_port);

    listen(initial_tcp_socket, 5);

    cout<<"Sending HELLO message"<<endl;

    string HELLO_MSG = "HELLO, UDP: " + to_string(sister_udp_port) + ", TCP: " + to_string(initial_tcp_port);
    int sent_bytes = sendto(main_request_socket, HELLO_MSG.c_str(), HELLO_MSG.size(), 0, (sockaddr*)&server_listening_addr, sizeof(server_listening_addr));
    if(sent_bytes <= 0){
        closesocket(initial_tcp_socket);
        closesocket(main_request_socket);
        closesocket(sister_udp_socket);
        return;
    }

    // select() is telling if the socket received something before timeout
    // bytes_received checks if the data received was -1 which happens when the socket disconnected.
    // in this case also, select will return 1 but bytes_received will be -1. so you need it still.

    // setsockopt doesnt work on accept, it works for recv timeouts. so accept still needs the select() func

    int my_chunks;
    total_chunks;
    // cout<<"Receiving chunks count"<<endl;
    SOCKET initial_tcp_socket_accepted = acceptWithTimeout(initial_tcp_socket, 60);
    if(initial_tcp_socket_accepted==INVALID_SOCKET){
        closesocket(initial_tcp_socket);
        closesocket(initial_tcp_socket_accepted);
        closesocket(main_request_socket);
        closesocket(sister_udp_socket);
        return;
    }
    char buf[1024] = {0};
    char ticket_buf[128] = {0};
    int bytes_received = 0;
    while(bytes_received < 128){
        int received = recv(initial_tcp_socket_accepted, ticket_buf+bytes_received, 128-bytes_received, 0);
        if(received == 0) break;
        if(received < 0){
            printErrorMessage(bytes_received);
            closesocket(initial_tcp_socket);
            closesocket(initial_tcp_socket_accepted);
            closesocket(main_request_socket);
            closesocket(sister_udp_socket);
            return;
        }
        bytes_received += received;
    }
    
    sscanf(ticket_buf, "TOTAL_CHUNKS: %d YOUR_CHUNKS: %d", &total_chunks, &my_chunks);

    bool chunk_tracker[100000];
    memset(chunk_tracker, false, 100000);

    thread sister(sister_thread, sister_udp_socket, filename, chunk_tracker, ref(mu));

    int chunks_received = 0;

    // cout<<"client id: "<<client_id<<" total chunks "<<total_chunks<<" my chunks "<<my_chunks<<endl;

    if(client_id==0) cout<<"Starting chunks loop"<<endl;
    while(chunks_received<my_chunks){
        FileChunkPacket packet;
        memset(packet.data, 0, 1024);
        packet.index = 0;
        packet.valid_bytes = 0;
        bytes_received = 0;
        if(client_id==0) cout<<"client id: "<<client_id<<" chunks received: "<<chunks_received<<endl;
        while(bytes_received<sizeof(packet)){
            // if(client_id==0) cout<<"client id: "<<client_id<<" bytes received: "<<bytes_received<<endl;
            int received = recv(initial_tcp_socket_accepted, ((char*)&packet)+bytes_received, sizeof(packet)-bytes_received, 0);
            // if(client_id==0) cout<<"client id: "<<client_id<<" received: "<<received<<endl;
            if(received ==0) break;
            if(received < 0){
                mu.lock();
                cout<<"client id: "<<client_id<<" returning early!"<<endl;
                mu.unlock();
                printErrorMessage(received);
                closesocket(initial_tcp_socket);
                closesocket(initial_tcp_socket_accepted);
                closesocket(main_request_socket);
                closesocket(sister_udp_socket);
                sister.join();
                return;
            }
            bytes_received += received;
        }
        if(client_id==0) cout<<"client id: "<<client_id<<" loop finished: "<<packet.index<<endl;
        mu.lock();
        std::fstream outfile(filename, std::ios::in | std::ios::out | std::ios::binary);
        outfile.seekp(packet.index*1024ULL);
        outfile.write(packet.data, packet.valid_bytes);
        chunk_tracker[packet.index] = true;
        outfile.close();
        mu.unlock();

        if(client_id==0) cout<<"client id: "<<client_id<<" received chunk: "<<packet.index<<endl;

        chunks_received++;
    }
    cout<<"client "<<client_id<<" sending RECEIVED_ALL message."<<endl;
    const char* RECEIVED_ALL_MSG = "RECEIVED_ALL";
    sent_bytes = sendto(main_request_socket, RECEIVED_ALL_MSG, strlen(RECEIVED_ALL_MSG), 0, (sockaddr*)&server_listening_addr, sizeof(server_listening_addr));

    cout<<"client "<<client_id<<" expecting sync complete"<<endl;
    memset(buf, 0, 1024);
    bytes_received = recvfrom(main_request_socket, buf, sizeof(buf)-1, 0, NULL, NULL);
    if(bytes_received < 0){
        // IMPORTANT ----------------------------- CHECK THIS LOGIC AGAIN 
        if(WSAGetLastError()==WSAECONNRESET){
            bytes_received = recvfrom(main_request_socket, buf, sizeof(buf)-1, 0, NULL, NULL);
        }
        else{
            cout<<"yo dude"<<endl;
            printErrorMessage(bytes_received);
            closesocket(initial_tcp_socket);
            closesocket(initial_tcp_socket_accepted);
            closesocket(main_request_socket);
            closesocket(sister_udp_socket);
            sister.join();
            return;
        }
    }
    if(strcmp(buf, "SYNC_COMPLETE")){
        cout<<"Message received is not SYNC_COMPLETE. Exiting.";
        closesocket(initial_tcp_socket);
        closesocket(initial_tcp_socket_accepted);
        closesocket(main_request_socket);
        closesocket(sister_udp_socket);
        sister.join();
        return;
    }
    cout<<"client "<<client_id<<" received SYNC_COMPLETE"<<endl;

    DWORD timeout_tcp_data_socket = 2000;
    for(int i=0; i<2*total_chunks; i++){
        if(chunk_tracker[i%total_chunks]==true) continue;

        FileChunkPacket packet;
        memset(packet.data, 0, 1024);
        packet.index = 0;
        packet.valid_bytes = 0;

        SOCKET tcp_data_socket_listener = socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in myaddress{};
        myaddress.sin_family = AF_INET;
        myaddress.sin_port = htons(0);
        myaddress.sin_addr.s_addr = INADDR_ANY;
        bind(tcp_data_socket_listener, (sockaddr*)&myaddress, sizeof(myaddress));
        socklen_t socklen = sizeof(myaddress);
        getsockname(tcp_data_socket_listener, (sockaddr*)&myaddress, &socklen);
        listen(tcp_data_socket_listener, 5);
        string REQUEST_MSG = "CHUNK: "+to_string(i%total_chunks)+", TCP: "+to_string(ntohs(myaddress.sin_port));
        mu.lock();
        cout<<"client : "<<client_id<<" "<<REQUEST_MSG<<endl;
        mu.unlock();
        
        sent_bytes = sendto(main_request_socket, REQUEST_MSG.c_str(), REQUEST_MSG.size(), 0, (sockaddr*)&server_listening_addr, sizeof(server_listening_addr));
        if(sent_bytes < 0){
            if(WSAGetLastError()==WSAEWOULDBLOCK){
                Sleep(50);
                i--;
                closesocket(tcp_data_socket_listener);
                continue;
            }
            else{
                cout<<"Couldn't . Exiting.";
                closesocket(initial_tcp_socket);
                closesocket(initial_tcp_socket_accepted);
                closesocket(main_request_socket);
                closesocket(sister_udp_socket);
                closesocket(tcp_data_socket_listener);
                sister.join();
                return;
            }
        }

        SOCKET tcp_data_socket = acceptWithTimeout(tcp_data_socket_listener, 5);
        if(tcp_data_socket==INVALID_SOCKET){
            cout<<"Invalid socket on request for chunk: "<<i%total_chunks<<endl;
            closesocket(tcp_data_socket_listener);
            i--;
            Sleep(500);
            continue;
        }
        setsockopt(tcp_data_socket, SOL_SOCKET, SO_RCVTIMEO, (char*)&timeout_tcp_data_socket, sizeof(timeout_tcp_data_socket));

        bytes_received = 0;
        while(bytes_received < sizeof(packet)){
            int bytes = recv(tcp_data_socket, ((char*)&packet)+bytes_received, sizeof(packet)-bytes_received, 0);
            if(bytes <= 0) break;
            bytes_received += bytes;
        }
        mu.lock();
        std::fstream outfile(filename, std::ios::in | std::ios::out | std::ios::binary);
        outfile.seekp(packet.index*1024ULL);
        outfile.write(packet.data, packet.valid_bytes);
        chunk_tracker[i%total_chunks] = true;
        outfile.close();
        mu.unlock();
        closesocket(tcp_data_socket);
        closesocket(tcp_data_socket_listener);
        cout<<"client "<<client_id<<"'s request for chunk "<<(i%total_chunks)<<" completed!"<<endl;
    }
    cout<<"client "<<client_id<<" DONE!"<<endl;
    string REQUEST_MSG = "DONE";
    sendto(main_request_socket, REQUEST_MSG.c_str(), REQUEST_MSG.size(), 0, (sockaddr*)&server_listening_addr, sizeof(server_listening_addr));

    sister.join();
    
    closesocket(main_request_socket);
    closesocket(initial_tcp_socket);
    closesocket(sister_udp_socket);
    closesocket(initial_tcp_socket_accepted);
}

int main(){
    WSADATA wsa;
    WSAStartup(MAKEWORD(2, 2), &wsa);

    N_CLIENTS = 5;
    vector <thread> clients;
    for(int i=0; i<N_CLIENTS; i++){
        clients.emplace_back(worker, i);
    }
    for(int i=0; i<N_CLIENTS; i++){
        clients[i].join();
    }

    WSACleanup();
}
