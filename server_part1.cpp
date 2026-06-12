#include <bits/stdc++.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#include <thread>
#include <mutex>

#pragma comment(lib, ws2_32)
using namespace std;

int N_CLIENTS;
mutex mu;
// mu is just a global 0/1 variable. everytime there is a line for mu.lock, the threads will check
// if the mu is already locked. it doesnt matter if theyre looking at different lines of code. its just a global
// pass flag.

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

// ---------------------------------------------------------------------------------------------------------------------
/*  can we make the braodcast using tcp, would that require n2 threads, because one socket isnt enough?
// ---------------------------------------------------------------------------------------------------------------------

You have hit the exact reason why network engineers strictly separate TCP and UDP for different tasks. Your intuition about the n2 thread explosion is 100% correct.

First, to be technically precise: you cannot "broadcast" with TCP. TCP is strictly a point-to-point, connection-oriented protocol. To send the same message to 100 clients using TCP, the server must physically establish 100 completely separate TCP connections. We call this "multi-unicast."

If you tried to replace the UDP broadcast with TCP multi-unicast for a cache miss, you would be forced to choose between two terrible architectural outcomes:
Scenario 1: The n2 Thread Explosion (Concurrency)

If you want to query all other clients at the exact same time so the original client doesn't have to wait long, your worker thread would have to spawn n−1 sub-threads.

    Each sub-thread creates a socket, calls connect(), sends the query, and waits for a response.

    If your server has n=100 clients connected, and they all happen to request a chunk that causes a cache miss at the exact same millisecond, you have 100 worker threads.

    Each of those 100 worker threads spawns 99 sub-threads to ask the others.

    Result: Your server instantly attempts to spawn and manage 9,900 concurrent threads. The operating system's CPU scheduler will choke, context-switching overhead will consume all your RAM, and the server will likely crash.

Scenario 2: Massive Latency (Sequential Blocking)

To avoid the thread explosion, you could force the single worker thread to handle all n−1 TCP connections sequentially inside a for loop.

    The thread creates socket 1, calls connect() (waiting for the 3-way handshake), sends the query, waits for the reply, and calls close().

    Then it moves to socket 2, then socket 3, up to socket 99.

    Result: Every single connect() call is a blocking network operation. If one of those clients has a slow network or drops packets, your worker thread gets stuck waiting for TCP timeouts. The original client who asked for the file chunk might have to wait several seconds (or minutes) just for the server to finish asking everyone else.

Why UDP is the Only Tool for the Job

This exact problem is why your assignment dictates UDP for the cache miss query.

With UDP, there is no 3-way handshake and no connection state. Your single worker thread creates exactly one UDP socket. It runs a fast for loop calling sendto() n−1 times, instantly pushing the datagrams into the OS outbound buffer.

It requires zero extra threads, zero handshakes, and takes a fraction of a millisecond to execute before the worker safely puts itself to sleep with recvfrom() to wait for the winner to reply.
 */

// ---------------------------------------------------------------------------------------------------------------------
/* 
 Why the Broadcast Socket Must Be Temporary
 // ---------------------------------------------------------------------------------------------------------------------

If you create a new UDP socket but make it permanent (e.g., a global broadcast_socket), you run into a massive thread concurrency nightmare.

Imagine two clients request different chunks at the exact same time, and both result in a cache miss.

    Worker Thread A broadcasts for Chunk 3.

    Worker Thread B broadcasts for Chunk 4.

    Both threads call recvfrom() on the global broadcast_socket and go to sleep.

    A peer replies with Chunk 4.

    The Fatal Flaw: The OS wakes up exactly one thread to receive the packet. It might wake up Worker A. Worker A (looking for Chunk 3) receives Chunk 4, realizes it's the wrong data, and drops it. Worker B remains stuck sleeping forever, and the TCP connection times out.
     */
/* 
// ---------------------------------------------------------------------------------------------------------------------
      also each must have a listening udp socket on at all the times right, to listen for broadcasts? does that mean each client thread will have a sister thread just for this?
      // ---------------------------------------------------------------------------------------------------------------------

You have hit the nail completely on the head. This is exactly where the architecture shifts from a simple client-server model into a true Peer-to-Peer (P2P) hybrid model.

To answer your questions directly: Yes, every client must have a permanent UDP socket listening at all times, and yes, that absolutely requires the client to spawn a dedicated "sister thread" running in the background.
 */
/*
// --------------------------------------------------------------------------------------------------------------------- 
  if i want to use the model where the client adds its listening port to the payload, then i must decide a port number in advance right? so i can reserver a pool of port numbers to use for this purpose later on?
// ---------------------------------------------------------------------------------------------------------------------

That is a very logical way to think about it, but manually managing a pool of ports is actually a network engineering anti-pattern. You do not need to reserve or hardcode anything!

The operating system already manages a massive pool of safe, available ports for you. You just need to know how to ask the OS to pick one, and then ask the OS which one it picked so you can write it into your payload.

Here is the industry-standard trick to handle this elegantly using Port 0 and a function called getsockname().
1. The "Port 0" Trick

When you set up the client's TCP listening socket, you populate your sockaddr_in structure just like normal, but you set the port to 0.

When you call bind() with port 0, it acts as a special flag to the operating system kernel. It tells the OS: "I need to listen for connections, but I don't care what port I use. Please reach into your ephemeral port pool and assign me the first available one."
2. Retrieving the Assigned Port

Once bind() returns successfully, your TCP socket is officially locked to a specific random port (e.g., 54321). But your C++ variables don't know that number yet.

To find out what port the OS just gave you, you use the getsockname() function. It reaches into the socket file descriptor and copies the actual network data back into your sockaddr_in struct.
The Exact Implementation Sequence

Here is the exact code your client's active thread should run before it sends the UDP request to the server:
C++

// 1. Create the Client's TCP Listening Socket
int tcp_listener = socket(AF_INET, SOCK_STREAM, 0);

struct sockaddr_in my_tcp_addr;
my_tcp_addr.sin_family = AF_INET;
my_tcp_addr.sin_addr.s_addr = INADDR_ANY;
my_tcp_addr.sin_port = 0; // THE TRICK: Ask OS for a random port

// 2. Bind the socket
bind(tcp_listener, (struct sockaddr*)&my_tcp_addr, sizeof(my_tcp_addr));

// 3. Ask the OS what port it just assigned
socklen_t len = sizeof(my_tcp_addr);
getsockname(tcp_listener, (struct sockaddr*)&my_tcp_addr, &len);

// 4. Extract the port and convert from network byte order
int assigned_tcp_port = ntohs(my_tcp_addr.sin_port);

// 5. Build your UDP payload dynamically!
std::string payload = "CHUNK:" + std::to_string(requested_chunk) + 
                      ",PORT:" + std::to_string(assigned_tcp_port);

// 6. Send the UDP request, then call listen() and accept() on tcp_listener
// ...

Why This is the Best Architecture

By using this method, your client becomes completely self-sufficient and collision-proof.

    You never have to worry about two clients on the same machine trying to use the same hardcoded port.

    You never have to maintain an array of "used" vs "free" ports.

    If a previous run of your program crashed and left a port stuck in the TIME_WAIT state, your new program won't fail to start, because the OS will just intelligently pick a different port.

You execute this sequence, construct your payload string, fire off the UDP packet, and immediately drop into your accept() call to wait for the server's TCP connection.

 */

set <pair<uint32_t, int>> clients; // for sister udp ports
set <pair <uint32_t, int>> clients_for_initial_download; 
set <pair <uint32_t, int>> clients_main_udp_ports; 

void worker(int offset, in_addr client_ip, int client_tcp_port){
    // since chunk requests come by udp, the server will have to initiate the tcp connection
    // with the client. 
    cout<<"Sending offset "<<offset<<" to client port "<<client_tcp_port<<endl;
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
        timeout.tv_sec = 2;                         // Set timer to 2 seconds
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
        int bytes_received = recv(reply_socket, (char*)&packet, sizeof(packet), 0);

        if (bytes_received <= 0) {
            std::cout << "Error: Peer connected but dropped the Wi-Fi." << std::endl;
            closesocket(reply_socket);
            closesocket(reply_listening_socket);
            closesocket(broadcast_socket);
            return; // Kill the thread safely
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

        connect(tcp_socket, (sockaddr*) &client_addr, sizeof(client_addr));

        send(tcp_socket, (char*)&packet, sizeof(packet), 0);
        // what about incomplete chunks with less than 1kb
        closesocket(tcp_socket);
        closesocket(broadcast_socket);
        closesocket(reply_socket);
        closesocket(reply_listening_socket);
    }
}

struct FileChunkPacket{
    int index;
    char data[1024];
    int valid_bytes;
};

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
        string TICKET_MSG = "TOTAL_CHUNKS: " + to_string(total_chunks) + " YOUR_CHUNKS: " + to_string(chunks_for_this_client);
        send(distributor_socket, TICKET_MSG.c_str(), TICKET_MSG.size(), 0);

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
    N_CLIENTS = 5;
    cache = new LRUCache(N_CLIENTS);
    // when clients first connect with server and receive their starting chunks, they must also tell their udp listening ports for broadcast. 

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

/*
Look at this exact block in your main() function:
C++

for(auto client: clients_main_udp_ports){
    // ...
    const char* SYNC_MSG = "SYNC_COMPLETE";
    sendto(main_listening_socket, SYNC_MSG, strlen(SYNC_MSG), 0, (sockaddr*)&client_udp_addr, sizeof(client_udp_addr));
}

If a client crashes or disconnects during the Phase 2 TCP file distribution, their UDP port is closed by the time Phase 2 ends.

    Your main thread fires the SYNC_COMPLETE message to that dead client using main_listening_socket.

    The packet bounces, and the OS sends an ICMP Port Unreachable error back.

    Windows logs this error strictly against main_listening_socket.

    Your main thread steps into the while(true) loop and calls recvfrom(main_listening_socket...).

    The trapped error immediately triggers, returning -1.
 */