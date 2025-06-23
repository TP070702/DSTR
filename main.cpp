#include "Transaction.hpp"
#include "nlohmann_json.hpp"

#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <algorithm>
#include <limits>
#include <chrono>

using namespace std;
using json = nlohmann::json;

#define MAX_TRANSACTIONS 10000
static const int PAGE_SIZE = 5;

// ------------------------------------------------------------------
// fixed‐size list for holding results
// ------------------------------------------------------------------
struct TransactionList {
    Transaction* data;
    int          count;
    int          capacity;

    TransactionList(int cap=1024)
      : data(new Transaction[cap]), count(0), capacity(cap)
    {}

    TransactionList(const TransactionList& o)
      : data(new Transaction[o.capacity]), count(o.count), capacity(o.capacity)
    {
      for (int i = 0; i < count; ++i)
        data[i] = o.data[i];
    }

    TransactionList(TransactionList&& o) noexcept
      : data(o.data), count(o.count), capacity(o.capacity)
    {
      o.data     = nullptr;
      o.count    = 0;
      o.capacity = 0;
    }

    TransactionList& operator=(const TransactionList& o) {
      if (this != &o) {
        delete[] data;
        capacity = o.capacity;
        count    = o.count;
        data     = new Transaction[capacity];
        for (int i = 0; i < count; ++i)
          data[i] = o.data[i];
      }
      return *this;
    }

    TransactionList& operator=(TransactionList&& o) noexcept {
      if (this != &o) {
        delete[] data;
        data     = o.data;
        count    = o.count;
        capacity = o.capacity;
        o.data     = nullptr;
        o.count    = 0;
        o.capacity = 0;
      }
      return *this;
    }

    ~TransactionList() {
      delete[] data;
    }

    void clear() {
      count = 0;
    }

    void push(const Transaction& t) {
      if (count == capacity) {
        int newCap = capacity * 2;
        Transaction* tmp = new Transaction[newCap];
        for (int i = 0; i < count; ++i)
          tmp[i] = data[i];
        delete[] data;
        data     = tmp;
        capacity = newCap;
      }
      data[count++] = t;
    }

    const Transaction& operator[](int i) const {
      return data[i];
    }
};

static TransactionList lastResults;
static string          lastLabel;
static bool            hasResults = false;

// ------------------------------------------------------------------
// ArrayStore: 1D array + quicksort + mergesort + binary searches
// ------------------------------------------------------------------
class ArrayStore {
private:
    Transaction* A;
    int          n;

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

    void merge(Transaction a[], int l, int m, int r) {
        int n1 = m - l + 1;
        int n2 = r - m;
        Transaction* L = new Transaction[n1];
        Transaction* R = new Transaction[n2];
        for (int i = 0; i < n1; ++i) L[i] = a[l + i];
        for (int j = 0; j < n2; ++j) R[j] = a[m + 1 + j];
        int i = 0, j = 0, k = l;
        while (i < n1 && j < n2)
            a[k++] = (L[i].location <= R[j].location ? L[i++] : R[j++]);
        while (i < n1) a[k++] = L[i++];
        while (j < n2) a[k++] = R[j++];
        delete[] L;
        delete[] R;
    }

    void mergeSort(Transaction a[], int l, int r) {
        if (l < r) {
            int m = l + (r - l) / 2;
            mergeSort(a, l, m);
            mergeSort(a, m + 1, r);
            merge(a, l, m, r);
        }
    }

public:
    ArrayStore(): A(new Transaction[MAX_TRANSACTIONS]), n(0) {}
    ~ArrayStore() { delete[] A; }

    void loadFromCSV(const string& fn) {
        ifstream f(fn);
        if (!f.is_open()) { cerr << "Cannot open " << fn << "\n"; return; }
        string line;
        getline(f, line); // header
        n = 0;
        while (n < MAX_TRANSACTIONS && getline(f,line)) {
            if (line.empty()) continue;
            stringstream ss(line);
            Transaction T;
            getline(ss, T.transaction_id, ',');
            getline(ss, T.timestamp, ',');
            getline(ss, T.sender_account, ',');
            getline(ss, T.receiver_account, ',');
            getline(ss, line, ',');  T.amount = line.empty() ? 0.0 : stod(line);
            getline(ss, T.transaction_type, ',');
            getline(ss, T.merchant_category, ',');
            getline(ss, T.location, ',');
            getline(ss, T.device_used, ',');
            getline(ss, line, ',');
            transform(line.begin(), line.end(), line.begin(), ::tolower);
            T.is_fraud = (line == "true");
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
        cout << "[Array] Loaded " << n << " rows\n";
    }

    int size() const { return n; }

    // linear searches
    TransactionList getByPaymentChannel(const string& ch) const {
        TransactionList out; out.clear();
        for (int i = 0; i < n; ++i)
            if (A[i].payment_channel == ch) out.push(A[i]);
        return out;
    }

    TransactionList getByTransactionType(const string& tp) const {
        TransactionList out; out.clear();
        for (int i = 0; i < n; ++i)
            if (A[i].transaction_type == tp) out.push(A[i]);
        return out;
    }

    TransactionList getByLocation(const string& loc) const {
        TransactionList out; out.clear();
        for (int i = 0; i < n; ++i)
            if (A[i].location == loc) out.push(A[i]);
        return out;
    }

    // binary searches
    TransactionList searchByPaymentChannelBinary(const string& key) {
        stable_sort(A, A+n, [](auto& a, auto& b){
            return a.payment_channel < b.payment_channel;
        });
        TransactionList out; 
        int lo=0, hi=n;
        while (lo<hi) {
            int mid=(lo+hi)/2;
            if (A[mid].payment_channel < key) lo=mid+1;
            else hi=mid;
        }
        while (lo < n && A[lo].payment_channel==key)
            out.push(A[lo++]);
        return out;
    }

    TransactionList searchByTransactionTypeBinary(const string& key) {
        stable_sort(A, A+n, [](auto& a, auto& b){
            return a.transaction_type < b.transaction_type;
        });
        TransactionList out;
        int lo=0, hi=n;
        while (lo<hi) {
            int mid=(lo+hi)/2;
            if (A[mid].transaction_type < key) lo=mid+1;
            else hi=mid;
        }
        while (lo < n && A[lo].transaction_type==key)
            out.push(A[lo++]);
        return out;
    }

    TransactionList searchByLocationBinary(const string& key) {
        stable_sort(A, A+n, [](auto& a, auto& b){
            return a.location < b.location;
        });
        TransactionList out;
        int lo=0, hi=n;
        while (lo<hi) {
            int mid=(lo+hi)/2;
            if (A[mid].location < key) lo=mid+1;
            else hi=mid;
        }
        while (lo < n && A[lo].location==key)
            out.push(A[lo++]);
        return out;
    }

    // quicksort
    void sortByLocation(bool asc=true) {
        quickSort(0, n-1);
        if (!asc) reverse(A, A+n);
        cout << "[Array] Quick-Sorted Location (" << (asc?"A-Z":"Z-A") << ")\n";
    }

    // mergesort
    void sortByLocationMerge(bool asc=true) {
        mergeSort(A, 0, n-1);
        if (!asc) reverse(A, A+n);
        cout << "[Array] Merge-Sorted Location (" << (asc?"A-Z":"Z-A") << ")\n";
    }

    void printFirstN(int k) const {
        cout<<"\n-- First "<<k<<" Rows --\n";
        for (int i = 0; i < k && i < n; ++i) {
            const auto& t = A[i];
            cout<<t.transaction_id
                <<" | "<<t.transaction_type
                <<" | "<<t.location
                <<" | "<<t.amount
                <<" | "<<t.merchant_category<<"\n";
        }
    }
};

// ------------------------------------------------------------------
// LinkedListStore: singly-linked list + quicksort + mergesort + binary
// ------------------------------------------------------------------
class LinkedListStore {
    struct Node {
        Transaction d;
        Node*       next;
        Node(const Transaction& x): d(x), next(nullptr) {}
    };

    Node* head;
    int   n;

    // quicksort-style list sort by location
    Node* quickSortList(Node* h) {
        if (!h || !h->next) return h;
        string pivot = h->d.location;
        Node *lH=nullptr,*lT=nullptr, *eH=nullptr,*eT=nullptr, *gH=nullptr,*gT=nullptr;
        for (Node* cur=h; cur; ) {
            Node* nx = cur->next; cur->next = nullptr;
            if      (cur->d.location < pivot) {
                if (!lH) lH=lT=cur; else lT->next=cur, lT=cur;
            }
            else if (cur->d.location == pivot) {
                if (!eH) eH=eT=cur; else eT->next=cur, eT=cur;
            }
            else {
                if (!gH) gH=gT=cur; else gT->next=cur, gT=cur;
            }
            cur = nx;
        }
        lH = quickSortList(lH);
        gH = quickSortList(gH);
        Node* nh=nullptr; Node* nt=nullptr;
        auto app=[&](Node* x){
            if (!x) return;
            if (!nh) nh=x; else nt->next=x;
            nt = x; while (nt->next) nt = nt->next;
        };
        app(lH); app(eH); app(gH);
        return nh;
    }

    // mergesort-style list sort by location
    Node* split(Node* h) {
        Node* slow=h;
        Node* fast=h->next;
        while (fast && fast->next) {
            slow = slow->next;
            fast = fast->next->next;
        }
        Node* second = slow->next;
        slow->next = nullptr;
        return second;
    }

    Node* mergeLists(Node* a, Node* b) {
        Node dummy{Transaction()};
        Node* tail = &dummy;
        while (a && b) {
            if (a->d.location <= b->d.location) {
                tail->next = a;
                a = a->next;
            } else {
                tail->next = b;
                b = b->next;
            }
            tail = tail->next;
        }
        tail->next = a ? a : b;
        return dummy.next;
    }

    Node* mergeSortList(Node* h) {
        if (!h || !h->next) return h;
        Node* second = split(h);
        h      = mergeSortList(h);
        second = mergeSortList(second);
        return mergeLists(h, second);
    }

    void reverseList() {
        Node* prev=nullptr;
        Node* cur = head;
        while(cur) {
            Node* nx = cur->next;
            cur->next = prev;
            prev = cur;
            cur  = nx;
        }
        head = prev;
    }

public:
    LinkedListStore(): head(nullptr), n(0) {}
    ~LinkedListStore(){
        while (head) {
            Node* t = head;
            head = head->next;
            delete t;
        }
    }

    void loadFromCSV(const string& fn) {
        ifstream f(fn);
        if (!f.is_open()) { cerr<<"Cannot open "<<fn<<"\n"; return; }
        string line; getline(f,line); 
        head=nullptr; n=0; Node* tail=nullptr;
        while (n < MAX_TRANSACTIONS && getline(f,line)) {
            if (line.empty()) continue;
            stringstream ss(line);
            Transaction T;
            getline(ss, T.transaction_id, ',');
            getline(ss, T.timestamp, ',');
            getline(ss, T.sender_account, ',');
            getline(ss, T.receiver_account, ',');
            getline(ss, line, ','); T.amount = line.empty()?0:stod(line);
            getline(ss, T.transaction_type, ',');
            getline(ss, T.merchant_category, ',');
            getline(ss, T.location, ',');
            getline(ss, T.device_used, ',');
            getline(ss, line, ',');
            transform(line.begin(), line.end(), line.begin(), ::tolower);
            T.is_fraud = (line=="true");
            getline(ss, T.fraud_type, ',');
            getline(ss, T.time_since_last_transaction, ',');
            getline(ss, T.spending_deviation_score, ',');
            getline(ss, line, ','); T.velocity_score = line.empty()?0:stod(line);
            getline(ss, line, ','); T.geo_anomaly_score = line.empty()?0:stod(line);
            getline(ss, T.payment_channel, ',');
            getline(ss, T.ip_address, ',');
            getline(ss, T.device_hash, ',');
            Node* nd = new Node(T);
            if (!head) head=tail=nd;
            else        tail->next=nd, tail=nd;
            ++n;
        }
        cout<<"[LL] Loaded "<<n<<" rows\n";
    }

    int size() const { return n; }

    // linear searches
    TransactionList getByPaymentChannel(const string& ch) const {
        TransactionList out; out.clear();
        for (Node* c=head; c; c=c->next)
            if (c->d.payment_channel == ch) out.push(c->d);
        return out;
    }

    TransactionList getByTransactionType(const string& tp) const {
        TransactionList out; out.clear();
        for (Node* c=head; c; c=c->next)
            if (c->d.transaction_type == tp) out.push(c->d);
        return out;
    }

    TransactionList getByLocation(const string& loc) const {
        TransactionList out; out.clear();
        for (Node* c=head; c; c=c->next)
            if (c->d.location == loc) out.push(c->d);
        return out;
    }

    // binary searches via flatten-and-sort
    TransactionList searchByPaymentChannelBinary(const string& key) const {
        // flatten
        TransactionList flat; 
        for (Node* c=head; c; c=c->next)
            flat.push(c->d);
        // sort
        stable_sort(flat.data, flat.data+flat.count, [](auto& a, auto& b){
            return a.payment_channel < b.payment_channel;
        });
        // binary search
        TransactionList out;
        int lo=0, hi=flat.count;
        while (lo<hi) {
            int mid=(lo+hi)/2;
            if (flat.data[mid].payment_channel < key) lo=mid+1;
            else hi=mid;
        }
        while (lo < flat.count && flat.data[lo].payment_channel==key)
            out.push(flat.data[lo++]);
        return out;
    }

    TransactionList searchByTransactionTypeBinary(const string& key) const {
        TransactionList flat; 
        for (Node* c=head; c; c=c->next)
            flat.push(c->d);
        stable_sort(flat.data, flat.data+flat.count, [](auto& a, auto& b){
            return a.transaction_type < b.transaction_type;
        });
        TransactionList out;
        int lo=0, hi=flat.count;
        while (lo<hi) {
            int mid=(lo+hi)/2;
            if (flat.data[mid].transaction_type < key) lo=mid+1;
            else hi=mid;
        }
        while (lo < flat.count && flat.data[lo].transaction_type==key)
            out.push(flat.data[lo++]);
        return out;
    }

    TransactionList searchByLocationBinary(const string& key) const {
        TransactionList flat; 
        for (Node* c=head; c; c=c->next)
            flat.push(c->d);
        stable_sort(flat.data, flat.data+flat.count, [](auto& a, auto& b){
            return a.location < b.location;
        });
        TransactionList out;
        int lo=0, hi=flat.count;
        while (lo<hi) {
            int mid=(lo+hi)/2;
            if (flat.data[mid].location < key) lo=mid+1;
            else hi=mid;
        }
        while (lo < flat.count && flat.data[lo].location==key)
            out.push(flat.data[lo++]);
        return out;
    }

    // quicksort
    void sortByLocation(bool asc=true) {
        head = quickSortList(head);
        if (!asc) reverseList();
        cout<<"[LL] Quick-Sorted Location ("<<(asc?"A-Z":"Z-A")<<")\n";
    }

    // mergesort
    void sortByLocationMerge(bool asc=true) {
        head = mergeSortList(head);
        if (!asc) reverseList();
        cout<<"[LL] Merge-Sorted Location ("<<(asc?"A-Z":"Z-A")<<")\n";
    }

    void printFirstN(int k) const {
        cout<<"\n-- First "<<k<<" Rows --\n";
        int i=0;
        for (Node* c=head; c && i<k; c=c->next, ++i) {
            const auto& t = c->d;
            cout<<t.transaction_id
                <<" | "<<t.transaction_type
                <<" | "<<t.location
                <<" | "<<t.amount
                <<" | "<<t.merchant_category<<"\n";
        }
    }
};

// ------------------------------------------------------------------
// pagination + search dispatch
// ------------------------------------------------------------------
void handleSearch(bool useArr,
                  ArrayStore& arr,
                  LinkedListStore& ll,
                  bool useBinary)
{
    const char* channels[] = {"card","ACH","UPI","wire_transfer"};
    const char* types[]    = {"deposit","transfer","withdrawal","payment"};

    while (true) {
        cout<<"\n-- SEARCH MENU --\n"
            <<"  1) By Payment Channel\n"
            <<"  2) By Transaction Type\n"
            <<"  3) By Location\n"
            <<"  4) Back\n"
            <<"Choose: ";
        int s; 
        if (!(cin>>s)) { cin.clear(); cin.ignore(1e9,'\n'); continue; }
        cin.ignore(1e9,'\n');
        if (s==4) break;

        TransactionList results; results.clear();
        string          criterion, label;
        // dispatch
        if (s==1) {
            cout<<"\nSelect channel:\n";
            for (int i=0;i<4;++i) cout<<"  "<<(i+1)<<") "<<channels[i]<<"\n";
            cout<<"  5) Back\nChoose: ";
            int pc; cin>>pc; cin.ignore(1e9,'\n');
            if (pc<1||pc>4) continue;
            criterion = channels[pc-1];
            label = "Channel="+criterion;
            auto start = chrono::high_resolution_clock::now();
            if (!useBinary) {
                results = useArr
                        ? arr.getByPaymentChannel(criterion)
                        : ll.getByPaymentChannel(criterion);
            } else {
                results = useArr
                        ? arr.searchByPaymentChannelBinary(criterion)
                        : ll.searchByPaymentChannelBinary(criterion);
            }
            auto stop  = chrono::high_resolution_clock::now();
            auto dur   = chrono::duration_cast<chrono::microseconds>(stop - start);
            size_t mem = useArr
                       ? arr.size()*sizeof(Transaction)
                       : ll.size()*(sizeof(Transaction)+sizeof(void*));
            cout<<"Time used by "<<(useArr?"Array":"Linked List")
                <<": "<<dur.count()<<" microseconds\n"
                <<"Memory used: "<<mem<<" bytes\n";
        }
        else if (s==2) {
            cout<<"\nSelect type:\n";
            for (int i=0;i<4;++i) cout<<"  "<<(i+1)<<") "<<types[i]<<"\n";
            cout<<"  5) Back\nChoose: ";
            int tt; cin>>tt; cin.ignore(1e9,'\n');
            if (tt<1||tt>4) continue;
            criterion = types[tt-1];
            label = "Type="+criterion;
            auto start=chrono::high_resolution_clock::now();
            if (!useBinary) {
                results = useArr
                        ? arr.getByTransactionType(criterion)
                        : ll.getByTransactionType(criterion);
            } else {
                results = useArr
                        ? arr.searchByTransactionTypeBinary(criterion)
                        : ll.searchByTransactionTypeBinary(criterion);
            }
            auto stop =chrono::high_resolution_clock::now();
            auto dur  =chrono::duration_cast<chrono::microseconds>(stop - start);
            size_t mem= useArr
                       ? arr.size()*sizeof(Transaction)
                       : ll.size()*(sizeof(Transaction)+sizeof(void*));
            cout<<"Time used by "<<(useArr?"Array":"Linked List")
                <<": "<<dur.count()<<" microseconds\n"
                <<"Memory used: "<<mem<<" bytes\n";
        }
        else if (s==3) {
            cout<<"Enter location: ";
            getline(cin, criterion);
            label = "Location="+criterion;
            auto start=chrono::high_resolution_clock::now();
            if (!useBinary) {
                results = useArr
                        ? arr.getByLocation(criterion)
                        : ll.getByLocation(criterion);
            } else {
                results = useArr
                        ? arr.searchByLocationBinary(criterion)
                        : ll.searchByLocationBinary(criterion);
            }
            auto stop =chrono::high_resolution_clock::now();
            auto dur  =chrono::duration_cast<chrono::microseconds>(stop - start);
            size_t mem= useArr
                       ? arr.size()*sizeof(Transaction)
                       : ll.size()*(sizeof(Transaction)+sizeof(void*));
            cout<<"Time used by "<<(useArr?"Array":"Linked List")
                <<": "<<dur.count()<<" microseconds\n"
                <<"Memory used: "<<mem<<" bytes\n";
        }
        else {
            cout<<"Invalid choice.\n";
            continue;
        }

        // save for export
        lastResults = move(results);
        lastLabel   = label;
        hasResults  = true;

        // paginate & display
        int total = lastResults.count;
        int pages = (total + PAGE_SIZE - 1) / PAGE_SIZE;
        if (pages == 0) { cout<<"(no records)\n"; continue; }
        int page = 0;
        while (true) {
            int startIdx = page * PAGE_SIZE;
            int endIdx   = min(startIdx + PAGE_SIZE, total);

            cout<<"\n-- "<< lastLabel <<" --\n";
            if (s==1) {
                cout<<"ID       | Channel        | Location     | Amount     | Merchant\n";
                cout<<"---------------------------------------------------------------\n";
                for (int i = startIdx; i < endIdx; ++i) {
                    auto& t = lastResults.data[i];
                    cout<<t.transaction_id
                        <<" | "<<t.payment_channel
                        <<" | "<<t.location
                        <<" | "<<t.amount
                        <<" | "<<t.merchant_category<<"\n";
                }
            }
            else if (s==2) {
                cout<<"ID       | Type           | Location     | Amount     | Merchant\n";
                cout<<"---------------------------------------------------------------\n";
                for (int i = startIdx; i < endIdx; ++i) {
                    auto& t = lastResults.data[i];
                    cout<<t.transaction_id
                        <<" | "<<t.transaction_type
                        <<" | "<<t.location
                        <<" | "<<t.amount
                        <<" | "<<t.merchant_category<<"\n";
                }
            }
            else {
                cout<<"ID       | Channel        | Type           | Location     | Amount     | Merchant\n";
                cout<<"-------------------------------------------------------------------------------\n";
                for (int i = startIdx; i < endIdx; ++i) {
                    auto& t = lastResults.data[i];
                    cout<<t.transaction_id
                        <<" | "<<t.payment_channel
                        <<" | "<<t.transaction_type
                        <<" | "<<t.location
                        <<" | "<<t.amount
                        <<" | "<<t.merchant_category<<"\n";
                }
            }

            cout<<"-- Page "<<(page+1)<<" of "<<pages<<" --\n"
                <<"Previous [1] | Next [2] | Back [3]\n"
                <<"Choose: ";
            int nav; cin>>nav; cin.ignore(1e9,'\n');
            if      (nav==1 && page>0)      --page;
            else if (nav==2 && page<pages-1)++page;
            else if (nav==3)                break;
        }
    }
}

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
            <<"Choose: ";
        int ds; 
        if (!(cin>>ds)) break;
        cin.ignore(1e9,'\n');
        if (ds==3) break;
        bool useArr = (ds==1);

        while (true) {
            cout<<"\n==== FEATURES ====\n"
                <<"1) Search (channel/type/location)\n"
                <<"2) Sort by Location\n"
                <<"3) Print First N Rows\n"
                <<"4) Export to JSON\n"
                <<"5) Back\n"
                <<"6) Exit\n"
                <<"Choose: ";
            int cmd; 
            if (!(cin>>cmd)) { cin.clear(); cin.ignore(1e9,'\n'); continue; }
            cin.ignore(1e9,'\n');
            if (cmd==5) break;
            if (cmd==6) return 0;

            switch (cmd) {
            case 1: {
                // ask linear vs binary
                cout<<"\nSelect search algorithm:\n"
                    <<"  1) Linear\n"
                    <<"  2) Binary\n"
                    <<"Choose: ";
                int alg; 
                cin>>alg; cin.ignore(1e9,'\n');
                bool useBinary = (alg==2);
                handleSearch(useArr, arr, ll, useBinary);
                break;
            }
            case 2: {
                // ask quick vs merge
                cout<<"\nChoose sorting algorithm:\n"
                    <<"  1) Quick Sort\n"
                    <<"  2) Merge Sort\n"
                    <<"Choose: ";
                int sa; cin>>sa; cin.ignore(1e9,'\n');
                // then ask order
                cout<<"\nSort order:\n"
                    <<"  1) A-Z\n"
                    <<"  2) Z-A\n"
                    <<"Choose: ";
                int d; cin>>d; cin.ignore(1e9,'\n');
                bool asc = (d != 2);

                auto start = chrono::high_resolution_clock::now();
                if (useArr) {
                    if (sa==1)      arr.sortByLocation(asc);
                    else            arr.sortByLocationMerge(asc);
                } else {
                    if (sa==1)      ll.sortByLocation(asc);
                    else            ll.sortByLocationMerge(asc);
                }
                auto stop  = chrono::high_resolution_clock::now();
                auto dur   = chrono::duration_cast<chrono::microseconds>(stop - start);
                size_t mem = useArr
                           ? arr.size()*sizeof(Transaction)
                           : ll.size()*(sizeof(Transaction)+sizeof(void*));
                cout<<"Time used by "<<(useArr?"Array":"Linked List")
                    <<": "<<dur.count()<<" microseconds\n"
                    <<"Memory used: "<<mem<<" bytes\n";
                break;
            }
            case 3: {
                cout<<"Rows to show: ";
                int k; cin>>k; cin.ignore(1e9,'\n');
                if (useArr) arr.printFirstN(k);
                else        ll.printFirstN(k);
                break;
            }
            case 4: {
                if (!hasResults) {
                    cout<<"No search results to export. Please run a search first.\n";
                    break;
                }
                cout<<"Enter output JSON filename: ";
                string fn; getline(cin, fn);
                json j = json::array();
                for (int i = 0; i < lastResults.count; ++i) {
                    auto& t = lastResults.data[i];
                    j.push_back({
                        {"transaction_id",    t.transaction_id},
                        {"payment_channel",   t.payment_channel},
                        {"transaction_type",  t.transaction_type},
                        {"location",          t.location},
                        {"amount",            t.amount},
                        {"merchant_category", t.merchant_category}
                    });
                }
                ofstream out(fn);
                out<<j.dump(4);
                cout<<"Exported "<<lastResults.count
                    <<" records ("<< lastLabel <<") to "<<fn<<"\n";
                break;
            }
            default:
                cout<<"Invalid choice.\n";
            }
        }
    }

    cout<<"Program terminated......\n";
    return 0;
}
