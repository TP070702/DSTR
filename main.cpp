#include "Transaction.hpp"
#include "nlohmann_json.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <algorithm>
#include <limits>
using namespace std;
using json = nlohmann::json;

#define MAX_TRANSACTIONS 10000

// ------------------------------------------------------------------
// ArrayStore: heap array + Quicksort on location + JSON export
// ------------------------------------------------------------------
class ArrayStore {
private:
    Transaction* A;
    int n;

    int partition(int low, int high) {
        const string& pivot = A[high].location;
        int i = low - 1;
        for (int j = low; j < high; ++j)
            if (A[j].location < pivot)
                swap(A[++i], A[j]);
        swap(A[i+1], A[high]);
        return i+1;
    }

    void quickSort(int low, int high) {
        if (low < high) {
            int pi = partition(low, high);
            quickSort(low, pi - 1);
            quickSort(pi + 1, high);
        }
    }

public:
    ArrayStore(): A(new Transaction[MAX_TRANSACTIONS]), n(0) {}
    ~ArrayStore() { delete[] A; }

    int size() const            { return n; }
    const Transaction& operator[](int i) const { return A[i]; }

    void loadFromCSV(const string& fn) {
        ifstream f(fn);
        if (!f.is_open()) { cerr<<"Cannot open "<<fn<<"\n"; return; }
        string line; getline(f,line); // skip header
        n = 0;
        while (n < MAX_TRANSACTIONS && getline(f,line)) {
            if (line.empty()) continue;
            stringstream ss(line), tmp;
            Transaction T;
            getline(ss, T.transaction_id, ',');
            getline(ss, T.timestamp, ',');
            getline(ss, T.sender_account, ',');
            getline(ss, T.receiver_account, ',');
            getline(ss, line, ',');  T.amount = line.empty()?0:stod(line);
            getline(ss, T.transaction_type, ',');
            getline(ss, T.merchant_category, ',');
            getline(ss, T.location, ',');
            getline(ss, T.device_used, ',');
            getline(ss, line, ',');  transform(line.begin(), line.end(), line.begin(), ::tolower);
                                      T.is_fraud = (line=="true");
            getline(ss, T.fraud_type, ',');
            getline(ss, T.time_since_last_transaction, ',');
            getline(ss, T.spending_deviation_score, ',');
            getline(ss, line, ',');  T.velocity_score = line.empty()?0:stod(line);
            getline(ss, line, ',');  T.geo_anomaly_score = line.empty()?0:stod(line);
            getline(ss, T.payment_channel, ',');
            getline(ss, T.ip_address, ',');
            getline(ss, T.device_hash, ',');

            A[n++] = T;
        }
        cout<<"[Array] Loaded "<<n<<" rows\n";
    }

    void filterByPaymentChannel(const string& ch) {
        cout<<"\n[Array] Channel="<<ch<<":\n";
        for (int i = 0; i < n; ++i)
            if (A[i].payment_channel == ch)
                cout<<A[i].transaction_id<<" | "<<A[i].amount<<" | "<<A[i].payment_channel<<"\n";
    }

    void sortByLocation(bool asc=true) {
        quickSort(0, n-1);
        if (!asc) reverse(A, A+n);
        cout<<"[Array] Sorted Loc ("<<(asc?"A-Z":"Z-A")<<")\n";
    }

    void searchByTransactionType(const string& tp) {
        cout<<"\n[Array] SearchType="<<tp<<":\n";
        bool found=false;
        for (int i = 0; i < n; ++i) {
            if (A[i].transaction_type == tp) {
                cout<<A[i].transaction_id<<" | "<<A[i].transaction_type<<"\n";
                found=true;
            }
        }
        if (!found) cout<<"  (no matches)\n";
    }

    void printFirstN(int k) {
        cout<<"\n[Array] First "<<k<<" rows:\n";
        for (int i = 0; i < k && i < n; ++i)
            cout<<A[i].transaction_id<<" | "<<A[i].location<<"\n";
    }

    void exportToJSON(const string& fn) {
        json j = json::array();
        for (int i = 0; i < n; ++i) {
            j.push_back({
                {"transaction_id",   A[i].transaction_id},
                {"payment_channel",  A[i].payment_channel},
                {"location",         A[i].location},
                {"transaction_type", A[i].transaction_type},
                {"amount",           A[i].amount}
            });
        }
        ofstream out(fn);
        out << j.dump(4);
        cout<<"[Array] Exported "<<n<<" records to "<<fn<<"\n";
    }

    void exportToJSONByPaymentChannel(const string& fn, const string& ch) {
        json j = json::array();
        for (int i = 0; i < n; ++i) {
            if (A[i].payment_channel == ch) {
                j.push_back({
                    {"transaction_id",   A[i].transaction_id},
                    {"payment_channel",  A[i].payment_channel},
                    {"location",         A[i].location},
                    {"transaction_type", A[i].transaction_type},
                    {"amount",           A[i].amount}
                });
            }
        }
        ofstream out(fn);
        out << j.dump(4);
        cout<<"[Array] Exported "<<j.size()<<" '"<<ch<<"' records to "<<fn<<"\n";
    }

    void exportToJSONByTransactionType(const string& fn, const string& tp) {
        json j = json::array();
        for (int i = 0; i < n; ++i) {
            if (A[i].transaction_type == tp) {
                j.push_back({
                    {"transaction_id",   A[i].transaction_id},
                    {"payment_channel",  A[i].payment_channel},
                    {"location",         A[i].location},
                    {"transaction_type", A[i].transaction_type},
                    {"amount",           A[i].amount}
                });
            }
        }
        ofstream out(fn);
        out << j.dump(4);
        cout<<"[Array] Exported "<<j.size()<<" '"<<tp<<"' records to "<<fn<<"\n";
    }
};

// ------------------------------------------------------------------
// LinkedListStore: single‐linked list + Quicksort + JSON export
// ------------------------------------------------------------------
class LinkedListStore {
    struct Node { Transaction d; Node* next; Node(const Transaction& x):d(x),next(nullptr){} };
    Node* head; int n;

    Node* quickSortList(Node* h) {
        if (!h||!h->next) return h;
        string pivot = h->d.location;
        Node *lH=nullptr,*lT=nullptr, *eH=nullptr,*eT=nullptr, *gH=nullptr,*gT=nullptr;
        for (Node* cur = h; cur; ) {
            Node* nx = cur->next; cur->next = nullptr;
            if (cur->d.location < pivot) {
                if (!lH) lH=lT=cur; else lT->next=cur, lT=cur;
            } else if (cur->d.location == pivot) {
                if (!eH) eH=eT=cur; else eT->next=cur, eT=cur;
            } else {
                if (!gH) gH=gT=cur; else gT->next=cur, gT=cur;
            }
            cur = nx;
        }
        lH = quickSortList(lH);
        gH = quickSortList(gH);
        Node* nh=nullptr; Node* nt=nullptr;
        auto app=[&](Node* x){
            if (!x) return;
            if (!nh) nh=x; else nt->next = x;
            nt=x; while (nt->next) nt=nt->next;
        };
        app(lH); app(eH); app(gH);
        return nh;
    }

public:
    LinkedListStore(): head(nullptr), n(0) {}
    ~LinkedListStore(){ while (head) { Node* t=head; head=head->next; delete t; } }

    int size() const { return n; }

    void loadFromCSV(const string& fn) {
        ifstream f(fn);
        if (!f.is_open()) { cerr<<"Cannot open "<<fn<<"\n"; return; }
        string line; getline(f,line);
        head=nullptr; n=0; Node* tail=nullptr;
        while (n < MAX_TRANSACTIONS && getline(f,line)) {
            if (line.empty()) continue;
            stringstream ss(line);
            string tmp; Transaction T;
            getline(ss,T.transaction_id,',');
            getline(ss,T.timestamp,',');
            getline(ss,T.sender_account,',');
            getline(ss,T.receiver_account,',');
            getline(ss,tmp,',');     T.amount = tmp.empty()?0:stod(tmp);
            getline(ss,T.transaction_type,',');
            getline(ss,T.merchant_category,',');
            getline(ss,T.location,',');
            getline(ss,T.device_used,',');
            getline(ss,tmp,',');     transform(tmp.begin(),tmp.end(),tmp.begin(),::tolower);
                                     T.is_fraud = (tmp=="true");
            getline(ss,T.fraud_type,',');
            getline(ss,T.time_since_last_transaction,',');
            getline(ss,T.spending_deviation_score,',');
            getline(ss,tmp,',');     T.velocity_score = tmp.empty()?0:stod(tmp);
            getline(ss,tmp,',');     T.geo_anomaly_score = tmp.empty()?0:stod(tmp);
            getline(ss,T.payment_channel,',');
            getline(ss,T.ip_address,',');
            getline(ss,T.device_hash,',');

            Node* nd = new Node(T);
            if (!head) head=tail=nd; else tail->next=nd, tail=nd;
            ++n;
        }
        cout<<"[LL] Loaded "<<n<<" rows\n";
    }

    void filterByPaymentChannel(const string& ch) {
        cout<<"\n[LL] Channel="<<ch<<":\n";
        for (Node* c=head; c; c=c->next)
            if (c->d.payment_channel==ch)
                cout<<c->d.transaction_id<<" | "<<c->d.amount<<" | "<<c->d.payment_channel<<"\n";
    }

    void sortByLocation(bool asc=true) {
        head = quickSortList(head);
        if (!asc) {
            Node* p=nullptr; Node* c=head;
            while (c) { Node* nx=c->next; c->next=p; p=c; c=nx; }
            head=p;
        }
        cout<<"[LL] Sorted Loc ("<<(asc?"A-Z":"Z-A")<<")\n";
    }

    void searchByTransactionType(const string& tp) {
        cout<<"\n[LL] SearchType="<<tp<<":\n";
        bool found=false;
        for (Node* c=head; c; c=c->next) {
            if (c->d.transaction_type==tp) {
                cout<<c->d.transaction_id<<" | "<<c->d.transaction_type<<"\n";
                found=true;
            }
        }
        if (!found) cout<<"  (no matches)\n";
    }

    void printFirstN(int k) {
        cout<<"\n[LL] First "<<k<<" rows:\n";
        int i=0;
        for (Node* c=head; c && i<k; c=c->next,++i)
            cout<<c->d.transaction_id<<" | "<<c->d.location<<"\n";
    }

    void exportToJSON(const string& fn) {
        json j = json::array();
        for (Node* c=head; c; c=c->next) {
            j.push_back({
                {"transaction_id",   c->d.transaction_id},
                {"payment_channel",  c->d.payment_channel},
                {"location",         c->d.location},
                {"transaction_type", c->d.transaction_type},
                {"amount",           c->d.amount}
            });
        }
        ofstream out(fn);
        out << j.dump(4);
        cout<<"[LL] Exported "<<n<<" records to "<<fn<<"\n";
    }

    void exportToJSONByPaymentChannel(const string& fn, const string& ch) {
        json j = json::array();
        for (Node* c=head; c; c=c->next) {
            if (c->d.payment_channel == ch) {
                j.push_back({
                    {"transaction_id",   c->d.transaction_id},
                    {"payment_channel",  c->d.payment_channel},
                    {"location",         c->d.location},
                    {"transaction_type", c->d.transaction_type},
                    {"amount",           c->d.amount}
                });
            }
        }
        ofstream out(fn);
        out << j.dump(4);
        cout<<"[LL] Exported "<<j.size()<<" '"<<ch<<"' records to "<<fn<<"\n";
    }

    void exportToJSONByTransactionType(const string& fn, const string& tp) {
        json j = json::array();
        for (Node* c=head; c; c=c->next) {
            if (c->d.transaction_type == tp) {
                j.push_back({
                    {"transaction_id",   c->d.transaction_id},
                    {"payment_channel",  c->d.payment_channel},
                    {"location",         c->d.location},
                    {"transaction_type", c->d.transaction_type},
                    {"amount",           c->d.amount}
                });
            }
        }
        ofstream out(fn);
        out << j.dump(4);
        cout<<"[LL] Exported "<<j.size()<<" '"<<tp<<"' records to "<<fn<<"\n";
    }
};

// ------------------------------------------------------------------
// main()
// ------------------------------------------------------------------
int main(){
    ArrayStore     arr;
    LinkedListStore ll;

    arr.loadFromCSV("financial_fraud_detection_dataset.csv");
    ll.loadFromCSV("financial_fraud_detection_dataset.csv");

    while (true) {
        cout<<"\n==== PICK DS ====\n"
            <<"1) Array-based\n"
            <<"2) Linked-list\n"
            <<"3) Exit\n"
            <<"Choose: "<<flush;
        int ds; if (!(cin>>ds)) break;
        cin.ignore(numeric_limits<streamsize>::max(),'\n');
        if (ds==3) break;
        bool useArr = (ds==1);

        while (true) {
            cout<<"\n==== FEATURES ====\n"
                <<"1) Filter by Payment Channel\n"
                <<"2) Sort by Location\n"
                <<"3) Search by Transaction Type\n"
                <<"4) Print First N Rows\n"
                <<"5) Export to JSON\n"
                <<"6) Back\n"
                <<"7) Exit\n"
                <<"Choose: "<<flush;
            int cmd; 
            if (!(cin>>cmd)) {
                cin.clear();
                cin.ignore(numeric_limits<streamsize>::max(),'\n');
                continue;
            }
            cin.ignore(numeric_limits<streamsize>::max(),'\n');

            if (cmd==7) return 0;
            if (cmd==6) break;

            switch (cmd) {
              case 1: {
                cout<<"Enter channel: "; string ch; getline(cin,ch);
                if (useArr) arr.filterByPaymentChannel(ch);
                else        ll.filterByPaymentChannel(ch);
                break;
              }
              case 2: {
                cout<<"\nSort order:\n 1) A-Z\n 2) Z-A\nChoose: "<<flush;
                int d; cin>>d; cin.ignore(numeric_limits<streamsize>::max(),'\n');
                bool asc = (d!=2);
                if (useArr) arr.sortByLocation(asc);
                else        ll.sortByLocation(asc);
                break;
              }
              case 3: {
                cout<<"Enter transaction type: "; string tp; getline(cin,tp);
                if (useArr) arr.searchByTransactionType(tp);
                else        ll.searchByTransactionType(tp);
                break;
              }
              case 4: {
                cout<<"Rows to show: "; int k; cin>>k; cin.ignore(numeric_limits<streamsize>::max(),'\n');
                if (useArr) arr.printFirstN(k);
                else        ll.printFirstN(k);
                break;
              }
              case 5: {
                // Export submenu
                cout<<"\nExport options:\n"
                    <<"  1) All records\n"
                    <<"  2) By Payment Channel\n"
                    <<"  3) By Transaction Type\n"
                    <<"Choose: "<<flush;
                int ex; cin>>ex; cin.ignore(numeric_limits<streamsize>::max(),'\n');
                cout<<"Enter output JSON filename: ";
                string fn; getline(cin, fn);

                if (ex==1) {
                    if (useArr) arr.exportToJSON(fn);
                    else        ll.exportToJSON(fn);
                }
                else if (ex==2) {
                    cout<<"Enter channel: "; string ch; getline(cin,ch);
                    if (useArr) arr.exportToJSONByPaymentChannel(fn,ch);
                    else        ll.exportToJSONByPaymentChannel(fn,ch);
                }
                else if (ex==3) {
                    cout<<"Enter transaction type: "; string tp; getline(cin,tp);
                    if (useArr) arr.exportToJSONByTransactionType(fn,tp);
                    else        ll.exportToJSONByTransactionType(fn,tp);
                }
                else {
                    cout<<"Invalid export option.\n";
                }
                break;
              }
              default:
                cout<<"Invalid choice.\n";
            }
        }
    }

    cout<<"Goodbye!\n";
    return 0;
}
