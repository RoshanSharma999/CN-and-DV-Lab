// Distance Vector Routing

#include <bits/stdc++.h>
using namespace std;

#define V 5
#define INF 9999

char nodes[V] = {'A','B','C','D','E'};

void distanceVector(int graph[V][V]) {
    int dist[V][V];
    int nextHop[V][V];

    for(int i=0;i<V;i++){
        for(int j=0;j<V;j++){
            dist[i][j] = graph[i][j];

            if (i == j || graph[i][j] == INF)
                nextHop[i][j] = -1;
            else
                nextHop[i][j] = j;
        }
    }

    cout << "\nSTEP 1: Direct Link Routing Tables\n";
    for(int i=0;i<V;i++){
        cout << "\nNode " << nodes[i] << " Routing Table\n";
        cout << "Dest\tCost\tNextHop\n";
        for(int j=0;j<V;j++){
            cout << nodes[j] << "\t";
            if(dist[i][j] == INF) cout << "INF\t";
            else cout << dist[i][j] << "\t";

            if(nextHop[i][j] == -1) cout << "-\n";
            else cout << nodes[nextHop[i][j]] << "\n";
        }
    }

    // Distance Vector Updates
    for(int k=0;k<V-1;k++){
        for(int i=0;i<V;i++){
            for(int j=0;j<V;j++){
                for(int via=0;via<V;via++){
                    if(dist[i][via] + dist[via][j] < dist[i][j]){
                        dist[i][j] = dist[i][via] + dist[via][j];
                        nextHop[i][j] = nextHop[i][via];
                    }
                }
            }
        }
    }

    cout << "\nSTEP 2: Final Shortest Path Routing Tables\n";
    for(int i=0;i<V;i++){
        cout << "\nNode " << nodes[i] << " Routing Table\n";
        cout << "Dest\tCost\tNextHop\n";
        for(int j=0;j<V;j++){
            cout << nodes[j] << "\t";
            if(dist[i][j] == INF) cout << "INF\t";
            else cout << dist[i][j] << "\t";

            if(nextHop[i][j] == -1) cout << "-\n";
            else cout << nodes[nextHop[i][j]] << "\n";
        }
    }
}

int main(){
    int graph[V][V] = {
        {0,   2,   INF, 1,   INF},
        {2,   0,   3,   2,   INF},
        {INF, 3,   0,   INF, 1},
        {1,   2,   INF, 0,   1},
        {INF, INF, 1,   1,   0}
    };

    distanceVector(graph);
    return 0;
}

/*

// simpler implementation of Dist Vec Routing(if next hop details aren't needed)

#include <bits/stdc++.h>
using namespace std;

#define INF 9999
#define N 5

int main() {
    char nodes[N] = {'A', 'B', 'C', 'D', 'E'};

    int cost[N][N] = {
        {0,   2,   INF, 1,   INF},
        {2,   0,   3,   2,   INF},
        {INF, 3,   0,   INF, 1},
        {1,   2,   INF, 0,   1},
        {INF, INF, 1,   1,   0}
    };

    char source;
    cout << "Enter source node (A-E): ";
    cin >> source;

    int src = source - 'A';

    vector<int> dist(N, INF);
    dist[src] = 0;

    // Distance Vector (Bellman-Ford) Algorithm
    for (int i = 0; i < N - 1; i++) {
        for (int u = 0; u < N; u++) {
            for (int v = 0; v < N; v++) {
                if (cost[u][v] != INF && dist[u] != INF &&
                    dist[u] + cost[u][v] < dist[v]) {
                    dist[v] = dist[u] + cost[u][v];
                }
            }
        }
    }

    cout << "\nShortest paths from node " << source << ":\n";
    for (int i = 0; i < N; i++) {
        cout << "To " << nodes[i] << " -> ";
        if (dist[i] == INF)
            cout << "INF\n";
        else
            cout << dist[i] << "\n";
    }

    return 0;
}

*/
