// Priotiy Queue to improve QoS

#include <bits/stdc++.h>
using namespace std;

// flowId is not necessary
struct Packet {
    int packetId;
    int flowId;
    int priority;
};

// Comparator: lower number = higher priority
struct comparePriority {
    bool operator()(Packet const &p1, Packet const &p2) {
        return p1.priority > p2.priority;
    }
};

int main() {
    priority_queue<Packet, vector<Packet>, comparePriority> pq;

    int n;
    cout << "Enter number of packets: ";
    cin >> n;

    cout << "\nEnter packet details:\n";
    cout << "(PacketID FlowID Priority)\n";
    string stfu;
    for (int i = 0; i < n; i++) {
        Packet p;
        cin >> p.packetId >> p.flowId >> p.priority;
        pq.push(p);
    }

    cout << "\nPacket transmission order using Priority Queuing:\n";

    while (!pq.empty()) {
        Packet p = pq.top();
        pq.pop();
        cout << "Packet " << p.packetId
             << " from Flow " << p.flowId
             << " with Priority " << p.priority << "\n";
    }

    return 0;
}
