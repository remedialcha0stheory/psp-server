#include <bits/stdc++.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#include <thread>
#include <mutex>

#pragma comment(lib, ws2_32)
using namespace std;

int N_CLIENTS;
mutex mu;

struct FileChunkPacket{
    int index;
    char data[1024];
    int valid_bytes;
};

struct DLLNode{
    char buf[1024];
    int offset;
    int valid_bytes;
    DLLNode* nextnode;
    DLLNode* prevnode;
};

class LRUCache{
public:
    map<int, DLLNode*> mp;
    int capacity;
    int curr;
    DLLNode* head;
    DLLNode* tail;

    LRUCache(int capacity){
        this->capacity = capacity;
        this->curr = 0;
        
        head = new DLLNode();
        tail = new DLLNode();

        head->nextnode = tail;
        head->prevnode = NULL;
        tail->prevnode = head;
        tail->nextnode = NULL;
    }

    const DLLNode* get(int offset){ // should this be const? how to make sure the returned node is not modified by the caller
        auto it = mp.find(offset);
        if (it == mp.end()) {
            return NULL; // Cache Miss
        }

        DLLNode* node = it->second; 

        node->prevnode->nextnode = node->nextnode;
        node->nextnode->prevnode = node->prevnode;

        head->nextnode->prevnode = node;
        node->prevnode = head;
        node->nextnode = head->nextnode;
        head->nextnode = node;
        
        return node;
    }

    void insert(const char* data, int offset, int valid_bytes){
        if(curr<capacity){
            curr++;
            DLLNode* newnode = new DLLNode();
            newnode->offset = offset;
            newnode->valid_bytes = valid_bytes;
            // COPY the data!
            memcpy(newnode->buf, data, 1024);
            newnode->nextnode = head->nextnode;
            newnode->prevnode = head;
            head->nextnode->prevnode = newnode;
            head->nextnode = newnode;
            mp[offset] = newnode;
        }
        else{
            DLLNode* newnode = tail->prevnode;
            mp.erase(newnode->offset);
            mp[offset] = newnode;
            newnode->offset = offset;
            newnode->valid_bytes = valid_bytes;
            // COPY the data!
            memcpy(newnode->buf, data, 1024);
            newnode->prevnode->nextnode = tail;
            tail->prevnode = newnode->prevnode;

            head->nextnode->prevnode = newnode;
            newnode->prevnode = head;
            newnode->nextnode = head->nextnode;
            head->nextnode = newnode;
        }
    }
};

LRUCache* cache;

set <pair<uint32_t, int>> clients; // for sister udp ports
set <pair <uint32_t, int>> clients_for_initial_download; 
set <pair <uint32_t, int>> clients_main_udp_ports; 

void worker(int offset, in_addr client_ip, int client_tcp_port){
    // since chunk requests come by udp, the server will have to initiate the tcp connection
    // with the client. 
    
    FileChunkPacket packet;
    packet.index = offset;
    mu.lock();
    const DLLNode* cachenode = cache->get(offset);
    char local_buf[1024];
    if(cachenode!=NULL){
        memcpy(packet.data, cachenode->buf, 1024);
        packet.valid_bytes = cachenode->valid_bytes;
    }
    mu.unlock();
    
    if(cachenode!=NULL){
        SOCKET tcp_socket = socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in client_addr{};
        client_addr.sin_family = AF_INET;
        client_addr.sin_port = htons(client_tcp_port);
        client_addr.sin_addr = client_ip;

        // should i start a tcp connection first and then look into the cache or first look in cache and then start connection?
        cout<<"CACHE HIT. Sending offset "<<offset<<" to client port "<<client_tcp_port<<endl;
        connect(tcp_socket, (sockaddr*) &client_addr, sizeof(client_addr));

        send(tcp_socket, (char*)&packet, sizeof(packet), 0);
        // what about incomplete chunks with less than 1kb
        closesocket(tcp_socket);
    }
    else{
        // broadcast to all clients.
        SOCKET broadcast_socket = socket(AF_INET, SOCK_DGRAM, 0);
        // we cant use a common braodcast udp socket for all worker threads because if multiple of them
        // broadcast at the same time a go to sleep (they asked for different chunks) and a reply comes
        // the OS doesnt know which udp socket this reply is for.
        SOCKET reply_listening_socket = socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in reply_address{};

        reply_address.sin_family = AF_INET;
        reply_address.sin_port = htons(0);
        reply_address.sin_addr.s_addr = INADDR_ANY;
        bind(reply_listening_socket, (sockaddr*)&reply_address, sizeof(reply_address));
        socklen_t socklen = sizeof(reply_address);
        getsockname(reply_listening_socket, (sockaddr*)&reply_address, &socklen);
        int assigned_listening_port = ntohs(reply_address.sin_port);

        // if we do not create and set to listen a reply port before sending broadcasts, a client could reply
        // while we're still sending broadcasts. this is why setting it to listen beforehand is needed.

        listen(reply_listening_socket, N_CLIENTS); // we must set this to listen mode before we make the broadcast.
        // this is the socket to which the requested chunk will be sent to.
        string query = "CHUNK: "+to_string(offset)+" REPLY_PORT: "+to_string(assigned_listening_port);
        for(auto client: clients){
            uint32_t ip_address = client.first;
            int port_num = client.second; // this has to be the sister thread's UDP listening socket port
            sockaddr_in client_addr{};
            client_addr.sin_family = AF_INET;
            client_addr.sin_port = htons(port_num);
            client_addr.sin_addr.s_addr = ip_address;

            sendto(broadcast_socket, query.c_str(), query.length(), 0, (sockaddr*)&client_addr, sizeof(client_addr));
        }

        // ----------------------------------------------------------------------------- Gemini code starts
        fd_set readfds; // an array of sockets
        // the FDs here can only be sockets on windows, can be other FDs like terminal, files etc on unix.
        FD_ZERO(&readfds);                          // Clear the timer array
        FD_SET(reply_listening_socket, &readfds);   // Put our socket in the sockets array

        timeval timeout;
        timeout.tv_sec = 5;                         // Set timer to 2 seconds
        timeout.tv_usec = 0;

        // select() puts the thread to sleep for exactly 2 seconds.
        // It wakes up instantly if a peer connects. Returns on the FIRST instance any socket is ready.
        int activity = select(0, &readfds, NULL, NULL, &timeout); // 0, array to sockets to read from, array of sockets to 
        // write to, array to read exceptions from, timeout 
        // activity = number of sockets in the provided 3 arrays that ended up ready for our task

        if (activity <= 0) {
            std::cout << "Timeout: Nobody has chunk " << offset << " yet." << std::endl;
            closesocket(reply_listening_socket);
            closesocket(broadcast_socket);
            return; // Kill the thread safely
        }
        // ----------------------------------------------------------------------------- Gemini code ends
        // this above block was completely ADDED by gemini, not a replacement for. earlier, i was going to this accept
        // line straight away 
        // now we accept some client's response to the broadcast
        SOCKET reply_socket = accept(reply_listening_socket, NULL, NULL);

        // ----------------------------------------------------------------------------- Gemini code starts
        DWORD recv_timeout_ms = 2000; 
        
        // setsockopt() changes OS rules. It tells Windows: 
        // "If recv() waits for more than 2000ms, abort and return -1."
        setsockopt(reply_socket, SOL_SOCKET, SO_RCVTIMEO, (const char*)&recv_timeout_ms, sizeof(recv_timeout_ms));

        // char buf[1024] = {0}; // Guarantee null-terminators
        int bytes_received = 0;
        while (bytes_received < sizeof(packet)) {
            int received = recv(reply_socket, ((char*)&packet) + bytes_received, sizeof(packet) - bytes_received, 0);
            if (received <= 0) break;
            bytes_received += received;
        }

        if (bytes_received < sizeof(packet)) {
            // The sister thread dropped the connection. Abort before poisoning the cache!
            closesocket(reply_socket);
            closesocket(reply_listening_socket);
            closesocket(broadcast_socket);
            return; 
        }
        // ----------------------------------------------------------------------------- Gemini code ends

        mu.lock();
        cache->insert(packet.data, offset, packet.valid_bytes);
        mu.unlock();

        // sending this chunk to the original client.
        SOCKET tcp_socket = socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in client_addr{};
        client_addr.sin_family = AF_INET;
        client_addr.sin_port = htons(client_tcp_port);
        client_addr.sin_addr = client_ip;

        // should i start a tcp connection first and then look into the cache or first look in cache and then start connection?
        cout<<"CACHE MISS. Sending offset "<<offset<<" to client port "<<client_tcp_port<<endl;
        connect(tcp_socket, (sockaddr*) &client_addr, sizeof(client_addr));

        send(tcp_socket, (char*)&packet, sizeof(packet), 0);
        // what about incomplete chunks with less than 1kb
        closesocket(tcp_socket);
        closesocket(broadcast_socket);
        closesocket(reply_socket);
        closesocket(reply_listening_socket);
    }
}



// this phenomenon called endianness - is for every integer that goes over a network, either port and ip
// in packet headers, or itnegers in the payload.
// we use htons and htonl for this. read more.

void distributeInitialChunks(){
    ifstream file("A2_small_file.txt", ios::binary | ios::ate);
    uintmax_t total_bytes = file.tellg();
    file.seekg(0);
    int total_chunks = (total_bytes+1023)/1024;
    int base_chunks_per_client = total_chunks/N_CLIENTS;
    int remaining_chunks = total_chunks%N_CLIENTS;
    
    int chunk_index = 0;
    int client_index = 0;
    for(auto client: clients_for_initial_download){
        int chunks_for_this_client = base_chunks_per_client + (client_index < remaining_chunks);
        int client_tcp_port = client.second;
        uint32_t client_ip = client.first;

        SOCKET distributor_socket = socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in client_addr{};
        client_addr.sin_family = AF_INET;
        client_addr.sin_port = htons(client_tcp_port);
        client_addr.sin_addr.s_addr = client_ip;

        connect(distributor_socket, (sockaddr*)&client_addr, sizeof(client_addr));

        // MOST IMPORTANT !!!!!!!!!!!!!!!!!!!! ------------------------------------------------------
        /* 
        this is super important, udp messages have strict message boundaries, tcp doesnt, the message lengths can be arbitrary.
        therefore you cant send this ticket message directly, you need to pad it safely inside a fixed sized 128 bytes buffer 
        and read at the client side exactly like it. a fixed protocol.
        */
        char TICKET_MSG[128] = {0};
        sprintf(TICKET_MSG, "TOTAL_CHUNKS: %d YOUR_CHUNKS: %d", total_chunks, chunks_for_this_client);
        int sent_bytes = 0;
        while(sent_bytes < 128){
            int sent = send(distributor_socket, TICKET_MSG+sent_bytes, 128-sent_bytes, 0);
            if(sent <= 0) break;
            sent_bytes += sent;
        }

        for(int i=0; i<chunks_for_this_client; i++){
            FileChunkPacket packet;
            packet.index = chunk_index;
            memset(packet.data, 0, 1024);
            file.read(packet.data, 1024);
            packet.valid_bytes = static_cast<int>(file.gcount());

            int total_sent = 0;
            // MOST IMPORTANT !!!!!!!!!!!!!!!!!!!! ------------------------------------------------------
            while (total_sent < sizeof(FileChunkPacket)) {
                int sent = send(distributor_socket, ((char*)&packet) + total_sent, sizeof(FileChunkPacket) - total_sent, 0);
                if (sent <= 0) break;
                total_sent += sent;
            }
            chunk_index++;
        }

        closesocket(distributor_socket);
        client_index++;
    }
    file.close();
    cout<<"File distributed successfully and closed!"<<endl;

}

int main(){
    N_CLIENTS = 100;
    cache = new LRUCache(N_CLIENTS);
    // when clients first connect with server and receive their starting chunks, they must also tell their udp listening ports for broadcast. 
    cout<<"Starting server.."<<endl;
    WSADATA wsa;
    WSAStartup(MAKEWORD(2, 2), &wsa);

    SOCKET main_listening_socket = socket(AF_INET, SOCK_DGRAM, 0);
    sockaddr_in listening_socket_addr{};
    listening_socket_addr.sin_family = AF_INET;
    listening_socket_addr.sin_addr.s_addr = INADDR_ANY;
    listening_socket_addr.sin_port = htons(8080);

    bind(main_listening_socket, (sockaddr*)&listening_socket_addr, sizeof(listening_socket_addr));

    // the worker threads are spawned per request basis, not per client basis. so each client's new request
    // will have a new thread. this means the client will have to create a new socket for each request.

    int curr_clients = 0;
    while(curr_clients<N_CLIENTS){
        sockaddr_in client_info{};
        char buf[1024] = {0};
        socklen_t length = sizeof(client_info);
        int bytes = recvfrom(main_listening_socket, buf, sizeof(buf)-1, 0, (sockaddr*)&client_info, &length);
        if(bytes <=0 ) continue;

        curr_clients++;
        int sister_udp_port;
        int initial_tcp_port;
        sscanf(buf, "HELLO, UDP: %d, TCP: %d", &sister_udp_port, &initial_tcp_port);
        clients.insert(make_pair(client_info.sin_addr.s_addr, sister_udp_port));
        clients_for_initial_download.insert(make_pair(client_info.sin_addr.s_addr, initial_tcp_port));
        clients_main_udp_ports.insert(make_pair(client_info.sin_addr.s_addr, client_info.sin_port));
    }

    distributeInitialChunks();

    cout<<"expecting RECEIVED_ALL messages"<<endl;
    curr_clients = 0;
    while(curr_clients<N_CLIENTS){
        char buf[1024] = {0};
        int bytes = recvfrom(main_listening_socket, buf, sizeof(buf)-1, 0, NULL, NULL);
        if(bytes <=0 ) continue;
        if(!strcmp(buf, "RECEIVED_ALL")) curr_clients++;
    }
    cout<<"received RECEIVED_ALL messages"<<endl;

    // needed if clients start transmitting requests and their duplicates in case of timeouts while server
    // was still distributing:

    for(auto client: clients_main_udp_ports){
        uint32_t client_ip = client.first;
        int main_port = client.second;
        sockaddr_in client_udp_addr{};
        client_udp_addr.sin_family=AF_INET;
        client_udp_addr.sin_port = main_port;
        client_udp_addr.sin_addr.s_addr = client.first;
        const char* SYNC_MSG = "SYNC_COMPLETE";
        sendto(main_listening_socket, SYNC_MSG, strlen(SYNC_MSG), 0, (sockaddr*)&client_udp_addr, sizeof(client_udp_addr));
    }
    
    int clients_done = 0;
    while(true){
        if(clients_done==N_CLIENTS) break;
        char buf[1024] = {0};
        sockaddr_in tcp_addr{}; // to retreive clients ip address
        socklen_t socklen = sizeof(tcp_addr);
        int bytes = recvfrom(main_listening_socket, buf, sizeof(buf)-1, 0, (sockaddr*)&tcp_addr, &socklen);
        if(bytes <=0 ) continue;
        
        if(strcmp(buf, "DONE")==0){
            clients_done++;
            continue;
        }
        int tcp_port;
        int offset;
        if(sscanf(buf, "CHUNK: %d, TCP: %d", &offset, &tcp_port)==2){
            thread(worker, offset, tcp_addr.sin_addr, tcp_port).detach();
        }
    }

    const char* shutdown_msg = "SHUTDOWN";
    SOCKET shutdown_broadcast_socket = socket(AF_INET, SOCK_DGRAM, 0);
    for(auto client: clients){
        sockaddr_in sister_addr{};
        sister_addr.sin_family = AF_INET;
        sister_addr.sin_port = htons(client.second);
        sister_addr.sin_addr.s_addr = client.first;

        sendto(shutdown_broadcast_socket, shutdown_msg, strlen(shutdown_msg), 0, (sockaddr*)&sister_addr, sizeof(sister_addr));
    }
    WSACleanup();
}
