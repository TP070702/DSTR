#include "Transaction.hpp"
#include "nlohmann_json.hpp"

#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <algorithm>
#include <limits>
#include <chrono>
#include <numeric>
#include <filesystem>
#include <iomanip>
#include <mutex>

#if defined(_WIN32)
  #include <windows.h>
  #include <psapi.h>
  // Returns current working set size (bytes)
  size_t getProcessRSS() {
      PROCESS_MEMORY_COUNTERS pmc;
      if (GetProcessMemoryInfo(GetCurrentProcess(), &pmc, sizeof(pmc))) {
          return pmc.WorkingSetSize;
      }
      return 0;
  }
#else
#include <unistd.h>
#include <fstream>
// Returns the *current* resident set size in bytes on Linux
size_t getProcessRSS() {
    std::ifstream statm("/proc/self/statm");
    long size_pages = 0, rss_pages = 0;
    if (!(statm >> size_pages >> rss_pages)) return 0;
    return rss_pages * static_cast<size_t>(sysconf(_SC_PAGESIZE));
}
#endif

using namespace std;
using json = nlohmann::json;

#define MAX_TRANSACTIONS 5000000
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
        delete[] data;
        capacity = 1024;
        data     = new Transaction[capacity];
        count    = 0;
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

    void exportToJSON(const string& fn, const string& title) const{
    {
        namespace fs = std::filesystem;

        fs::path exportDir = fs::current_path() / "export-files";
        fs::create_directories(exportDir);

        fs::path filePath = exportDir / fn;

        json j = json::array();
        json header;
        header["title"] = title;
        j.push_back(header);

        for (int i = 0; i < count; ++i) {
            const auto& t = data[i];
            json entry = {
                { "transaction_id",    t.transaction_id },
                { "payment_channel",   t.payment_channel },
                { "transaction_type",  t.transaction_type },
                { "location",          t.location },
                { "amount",            t.amount },
                { "merchant_category", t.merchant_category }
            };
            j.push_back(entry);
        }

        ofstream out(filePath);
        out << j.dump(4);
        }
    }
};

static TransactionList lastResults;
static string          lastLabel;
static bool            hasResults = false;

// ------------------------------------------------------------------
// ArrayStore: 1D array + quicksort + mergesort + binary searches
// ------------------------------------------------------------------
class ArrayStore {
    Transaction* A;
    int* idx;
    int n;
    TransactionList channels[4];
    string lastChannel;
    static const char* NAMES[4];

    static int indexOf(const string& ch) {
        for (int i = 0; i < 4; ++i)
            if (ch == NAMES[i]) return i;
        return -1;
    }
private:
    int partitionIdx(int idx[], int low, int high) {
        auto pivot = A[idx[high]].location;
        int i = low - 1;
        for (int j = low; j < high; ++j) {
            if (A[idx[j]].location < pivot) {
            swap(idx[++i], idx[j]);
            }
        }
        swap(idx[i+1], idx[high]);
        return i+1;
    }

    void quickSortIdx(int idx[], int low, int high) {
        if (low >= high) return;
        string pivot = A[idx[low]].location;

        int lt = low, i = low, gt = high;
        while (i <= gt) {
            if      (A[idx[i]].location <  pivot) swap(idx[lt++], idx[i++]);
            else if (A[idx[i]].location >  pivot) swap(idx[i]    , idx[gt--]);
            else                                   ++i;
        }
        quickSortIdx(idx, low,    lt - 1);
        quickSortIdx(idx, gt + 1, high);
    }

    void merge(int idx[], int tmp[], int l, int m, int r) {
        int i = l, j = m+1, k = l;
        while (i <= m && j <= r) {
            if (A[idx[i]].location <= A[idx[j]].location)
            tmp[k++] = idx[i++];
            else
            tmp[k++] = idx[j++];
        }
        while (i <= m) tmp[k++] = idx[i++];
        while (j <= r) tmp[k++] = idx[j++];
        for (int t = l; t <= r; ++t) idx[t] = tmp[t];
    }

    void mergeSort(int idx[], int tmp[], int l, int r) {
        if (l >= r) return;
        int m = l + (r-l)/2;
        mergeSort(idx, tmp, l,   m);
        mergeSort(idx, tmp, m+1, r);
        merge(idx, tmp, l, m, r);
    }

public:
    ArrayStore()
      : A(new Transaction[MAX_TRANSACTIONS])
      , idx(new int[MAX_TRANSACTIONS])
      , n(0)
      , channels{ TransactionList(),TransactionList(),
                  TransactionList(),TransactionList() }
    {}

    ~ArrayStore() {
        delete[] A;
        delete[] idx;
    }

     void loadAllFromCSV(const string& fn) {
        n = 0;
        for (int i = 0; i < 4; ++i) channels[i].clear();
        lastChannel.clear();

        ifstream f(fn);
        if (!f.is_open()) {
            std::cerr << "Cannot open " << fn << "\n";
            return;
        }

        string line;
        getline(f, line);          // skip header
        int totalRead = 0;

        while (getline(f, line) && totalRead < MAX_TRANSACTIONS) {
            if (line.empty()) continue;
            ++totalRead;

            stringstream ss(line);
            Transaction T;
            getline(ss, T.transaction_id, ',');
            getline(ss, T.timestamp,      ',');
            getline(ss, T.sender_account, ',');
            getline(ss, T.receiver_account, ',');
            getline(ss, line,             ','); T.amount = line.empty() ? 0 : std::stod(line);
            getline(ss, T.transaction_type,   ',');
            getline(ss, T.merchant_category,  ',');
            getline(ss, T.location,           ',');
            getline(ss, T.device_used,        ',');
            getline(ss, line,                 ','); std::transform(line.begin(), line.end(), line.begin(), ::tolower);
            T.is_fraud = (line == "true");
            getline(ss, T.fraud_type,                ',');
            getline(ss, T.time_since_last_transaction, ',');
            getline(ss, T.spending_deviation_score,  ',');
            getline(ss, line,                         ','); T.velocity_score    = line.empty() ? 0 : std::stod(line);
            getline(ss, line,                         ','); T.geo_anomaly_score = line.empty() ? 0 : std::stod(line);
            getline(ss, T.payment_channel, ',');
            getline(ss, T.ip_address,      ',');
            getline(ss, T.device_hash,     ',');

            int ci = indexOf(T.payment_channel);
            if (ci < 0) continue;

            channels[ci].push(T);

            A[n]   = T;
            idx[n] = n;
            ++n;
        }

        iota(idx, idx + n, 0);
        cout << "[Array] Loaded " << n << " rows (full) | Distribution: ";
        for (int i = 0; i < 4; ++i)
            cout << NAMES[i] << ":" << channels[i].count << " ";
        cout << "\n";
    }

    void loadFromCSV(const string& fn, const string& channel) {
        for (int i = 0; i < 4; ++i) {
            channels[i].clear();
        }
        lastChannel = channel;
        n = 0;

        int sel = indexOf(channel);
        if (sel < 0) {
            cerr<<"Unknown channel: "<<channel<<"\n";
            return;
        }

        ifstream f(fn);
        if (!f.is_open()) {
            cerr<<"Cannot open "<<fn<<"\n";
            return;
        }
        string line;
        getline(f, line);  // skip header

        int totalRead = 0;
        while (getline(f, line) && totalRead < MAX_TRANSACTIONS) {
            if (line.empty()) continue;
            totalRead++;

            stringstream ss(line);
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
            getline(ss, line, ','); transform(line.begin(),line.end(),line.begin(),::tolower);
            T.is_fraud = (line=="true");
            getline(ss, T.fraud_type, ',');
            getline(ss, T.time_since_last_transaction, ',');
            getline(ss, T.spending_deviation_score, ',');
            getline(ss, line, ',');  T.velocity_score    = line.empty()?0:stod(line);
            getline(ss, line, ',');  T.geo_anomaly_score = line.empty()?0:stod(line);
            getline(ss, T.payment_channel, ',');
            getline(ss, T.ip_address, ',');
            getline(ss, T.device_hash, ',');

            int ci = indexOf(T.payment_channel);
            if (ci < 0) continue;

            channels[ci].push(T);

            if (ci == sel && n < MAX_TRANSACTIONS) {
                A[n] = T;
                idx[n] = n;
                ++n;
            }
        }

        iota(idx, idx + n, 0);
        cout << "[Array] Loaded " << n << " rows | Payment-Channel: " << channel << " | Distribution: ";
        for (int i = 0; i < 4; ++i) {
            cout << NAMES[i] << ":" << channels[i].count << " ";
        }
        cout << "\n";
    }

    int size() const { return n; }

    TransactionList getByPaymentChannel(int choice) const {
        int ci = choice - 1;
        if (ci < 0 || ci >= 4)
            throw runtime_error("Bad channel choice");
        return channels[ci];
    }

    // linear searches
    TransactionList getByTransactionType(const string& tp) const {
        TransactionList out;
        for (int k = 0; k < n; ++k) {
            const auto &t = A[idx[k]];
            if (t.transaction_type == tp)
                out.push(t);
        }
        return out;
    }
    TransactionList getByLocation(const string& loc) const {
        TransactionList out;
        for (int k = 0; k < n; ++k) {
            const auto &t = A[idx[k]];
            if (t.location == loc)
                out.push(t);
        }
        return out;
    }

    // binary searches
    TransactionList searchByTransactionTypeBinary(const string& key) {
        iota(idx, idx+n, 0);
        stable_sort(idx, idx+n,
            [&](int a,int b){ return A[a].transaction_type < A[b].transaction_type; });
        int lo=0, hi=n;
        while (lo<hi) {
            int mid=(lo+hi)/2;
            if (A[idx[mid]].transaction_type < key) lo=mid+1;
            else hi=mid;
        }
        TransactionList out;
        while (lo<n && A[idx[lo]].transaction_type==key)
            out.push(A[idx[lo++]]);
        return out;
    }
    TransactionList searchByLocationBinary(const string& key) {
        iota(idx, idx+n, 0);
        stable_sort(idx, idx+n,
            [&](int a,int b){ return A[a].location < A[b].location; });
        int lo=0, hi=n;
        while (lo<hi) {
            int mid=(lo+hi)/2;
            if (A[idx[mid]].location < key) lo=mid+1;
            else hi=mid;
        }
        TransactionList out;
        while (lo<n && A[idx[lo]].location==key)
            out.push(A[idx[lo++]]);
        return out;
    }

    // quick-sort
    void sortByLocation(bool asc = true) {
        for (int i = 0; i < n; ++i) idx[i] = i;
        quickSortIdx(idx, 0, n-1);       // your 3-way on idx[]
        if (!asc) std::reverse(idx, idx+n);
        cout<<"[Array] Index-QuickSort Location ("<<(asc?"A-Z":"Z-A")<<")\n";
    }

    // merge-sort
    void sortByLocationMerge(bool asc = true) {
        for (int i = 0; i < n; ++i) idx[i] = i;
        
        int* tmp = new int[n];
        mergeSort(idx, tmp, 0, n-1);
        // delete[] tmp;

        if (!asc) {
        for (int i = 0; i < n/2; ++i)
            std::swap(idx[i], idx[n-1-i]);
        }

        cout << "[Array] Index-Merge Location ("
            << (asc ? "A-Z" : "Z-A") << ")\n";
    }

    void exportToJSON(const std::string& fn, const std::string& title) const {
        namespace fs = std::filesystem;

        fs::path exportDir = fs::current_path() / "export-files";
        fs::create_directories(exportDir);

        fs::path filePath = exportDir / fn;

        json j = json::array();
        json header;
        header["title"] = title;
        j.push_back(header);

        for (int i = 0; i < n; ++i) {
            const auto& t = A[idx[i]];
            json entry = {
                {"transaction_id",    t.transaction_id},
                {"payment_channel",   t.payment_channel},
                {"transaction_type",  t.transaction_type},
                {"location",          t.location},
                {"amount",            t.amount},
                {"merchant_category", t.merchant_category}
            };
            j.push_back(entry);
        }

        ofstream out(filePath);
        out << j.dump(4);
    }

    // paginate & export
    void printFirstN(int limit) const {
        int total = min(limit, n);
        int pages = (total + PAGE_SIZE - 1) / PAGE_SIZE;
        if (!pages) {
            cout << "(no records)\n";
            return;
        }

        int page = 0;
        while (true) {
            int start = page * PAGE_SIZE;
            int end   = min(start + PAGE_SIZE, total);

            cout << "\n-- Showing " << total
                << " Rows (Page " << page+1 << "/" << pages << ") --\n"
                << left
                << setw(10) << "ID"
                << "| " << setw(15) << "Type"
                << "| " << setw(13) << "Channel"
                << "| " << setw(12) << "Location"
                << "| " << setw(10) << "Amount"
                << "| " << setw(12) << "Merchant\n"
                << string(65, '-') << "\n";

            for (int i = start; i < end; ++i) {
            const auto& t = A[idx[i]];
            cout << setw(10) << t.transaction_id
                << "| " << setw(15) << t.transaction_type
                << "| " << setw(13) << t.payment_channel
                << "| " << setw(12) << t.location
                << "| " << setw(10) << fixed << setprecision(2) << t.amount
                << "| " << setw(12) << t.merchant_category << "\n";
            }

            cout << "-- Page " << page+1 << " of " << pages << " --\n"
                << "Previous [1] | Next [2] | Back [3] | Jump [4] | Export to JSON [5]\n"
                << "Choose: ";
            int cmd; cin >> cmd; cin.ignore(numeric_limits<streamsize>::max(), '\n');

            if (cmd == 1 && page > 0)          --page;
            else if (cmd == 2 && page < pages-1) ++page;
            else if (cmd == 3)                  return;
            else if (cmd == 4) {
            cout << "Page (1-" << pages << "): ";
            int p; cin >> p; cin.ignore(numeric_limits<streamsize>::max(), '\n');
            if (p >= 1 && p <= pages) page = p - 1;
            }
            else if (cmd == 5) {
            cout << "Enter JSON filename: ";
            string fn; getline(cin, fn);
            if (!fn.empty()) {
                string title = "[Array] Split - Channel: " + lastChannel;
                exportToJSON(fn, title);
                cout << "Exported " << total << " rows to " << fn << "\n";
            }
            }
            else {
            cout << "...Invalid option.\n";
            }
        }
    }

    void reset() {
        delete[] A;
        delete[] idx;

        A   = new Transaction[MAX_TRANSACTIONS];
        idx = new int[MAX_TRANSACTIONS];

        n = 0;
        lastChannel.clear();
        for (int i = 0; i < 4; ++i) {
        channels[i] = TransactionList();
        }
    }

};
const char* ArrayStore::NAMES[4] = {
    "card","ACH","UPI","wire_transfer"
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
    Node* tail;
    int n;
    string lastChannel;
    TransactionList channels[4];
    static const char* NAMES[4];

    static int indexOf(const string& ch) {
        for (int i = 0; i < 4; ++i)
        if (ch == NAMES[i]) return i;
        return -1;
    }

    // quicksort
    Node* quickSortList(Node* h) {
        if (!h || !h->next) return h;
        string pivot = h->d.location;
        Node *lH=nullptr,*lT=nullptr, *eH=nullptr,*eT=nullptr, *gH=nullptr,*gT=nullptr;
        for (Node* cur=h; cur; ) {
            Node* nx = cur->next; cur->next = nullptr;
            if      (cur->d.location < pivot) {
                if (!lH) lH=lT=cur;
                else      lT->next=cur, lT=cur;
            }
            else if (cur->d.location == pivot) {
                if (!eH) eH=eT=cur;
                else      eT->next=cur, eT=cur;
            }
            else {
                if (!gH) gH=gT=cur;
                else      gT->next=cur, gT=cur;
            }
            cur = nx;
        }
        lH = quickSortList(lH);
        gH = quickSortList(gH);
        Node* nh=nullptr; Node* nt=nullptr;
        auto app=[&](Node* x){
            if (!x) return;
            if (!nh) nh=x;
            else      nt->next=x;
            nt = x; while (nt->next) nt = nt->next;
        };
        app(lH); app(eH); app(gH);
        return nh;
    }

    // mergesort
    Node* split(Node* h) {
        Node* slow = h;
        Node* fast = h->next;
        while (fast && fast->next) {
            slow = slow->next;
            fast = fast->next->next;
        }
        Node* second = slow->next;
        slow->next = nullptr;
        return second;
    }

    Node* mergeLists(Node* a, Node* b) {
        Node dummy{Transaction()}; Node* tail=&dummy;
        while (a && b) {
            if (a->d.location <= b->d.location) {
                tail->next = a; a=a->next;
            } else {
                tail->next = b; b=b->next;
            }
            tail = tail->next;
        }
        tail->next = a? a : b;
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
    LinkedListStore(): head(nullptr), tail(nullptr), n(0),
        channels{ TransactionList(), TransactionList(),
                  TransactionList(), TransactionList() }
    {}
    ~LinkedListStore(){
        while (head) {
            Node* t = head;
            head = head->next;
            delete t;
        }
    }

    void loadAllFromCSV(const string& fn) {
        while (head) {
            Node* t = head;
            head = head->next;
            delete t;
        }
        head = tail = nullptr;
        n = 0;
        for (int i = 0; i < 4; ++i) channels[i].clear();
        lastChannel.clear();

        ifstream f(fn);
        if (!f.is_open()) {
            cerr << "Cannot open " << fn << "\n";
            return;
        }

        string line;
        getline(f, line);  // skip header
        int totalRead = 0;

        while (getline(f, line) && totalRead < MAX_TRANSACTIONS) {
            if (line.empty()) continue;
            ++totalRead;

            stringstream ss(line);
            Transaction T;
            getline(ss, T.transaction_id, ',');
            getline(ss, T.timestamp,      ',');
            getline(ss, T.sender_account, ',');
            getline(ss, T.receiver_account, ',');
            getline(ss, line,               ','); T.amount = line.empty()?0:stod(line);
            getline(ss, T.transaction_type, ',');
            getline(ss, T.merchant_category, ',');
            getline(ss, T.location,           ',');
            getline(ss, T.device_used,        ',');
            getline(ss, line,                 ','); transform(line.begin(), line.end(), line.begin(), ::tolower);
            T.is_fraud = (line=="true");
            getline(ss, T.fraud_type,                ',');
            getline(ss, T.time_since_last_transaction, ',');
            getline(ss, T.spending_deviation_score,  ',');
            getline(ss, line,                         ','); T.velocity_score    = line.empty()?0:stod(line);
            getline(ss, line,                         ','); T.geo_anomaly_score = line.empty()?0:stod(line);
            getline(ss, T.payment_channel, ',');
            getline(ss, T.ip_address,      ',');
            getline(ss, T.device_hash,     ',');

            int ci = indexOf(T.payment_channel);
            if (ci >= 0) channels[ci].push(T);

            Node* nd = new Node(T);
            if (!head) head = tail = nd;
            else       tail->next = nd, tail = nd;
            ++n;
        }

        cout << "[LL] Loaded " << n << " rows (full) | Distribution: ";
        for (int i = 0; i < 4; ++i) {
            cout << NAMES[i] << ":" << channels[i].count << " ";
        }
        cout << "\n";
    }

    void exportToJSON(const std::string& fn, const std::string& title) const {
        namespace fs = std::filesystem;

        fs::path exportDir = fs::current_path() / "export-files";
        fs::create_directories(exportDir);

        fs::path filePath = exportDir / fn;

        json j = json::array();
        json header;
        header["title"] = title;
        j.push_back(header);

        for (Node* c = head; c; c = c->next) {
            const auto& t = c->d;
            json entry = {
                {"transaction_id",    t.transaction_id},
                {"payment_channel",   t.payment_channel},
                {"transaction_type",  t.transaction_type},
                {"location",          t.location},
                {"amount",            t.amount},
                {"merchant_category", t.merchant_category}
            };
            j.push_back(entry);
        }

        ofstream out(filePath);
        out << j.dump(4);
    }

    void loadFromCSV(const string& fn, const string& channel) {
        while (head) {
            Node* t = head;
            head = head->next;
            delete t;
        }
        head = tail = nullptr;
        n = 0;

        for (int i = 0; i < 4; ++i) {
            channels[i].clear();
        }
        lastChannel=channel;

        ifstream f(fn);
        if (!f.is_open()) { cerr<<"Cannot open "<<fn<<"\n"; return  ; }
        string line; getline(f,line);
        head = nullptr; n = 0; Node* tail = nullptr;
        while (n < MAX_TRANSACTIONS && getline(f,line)) {
            if (line.empty()) continue;
            stringstream ss(line);
            Transaction T;
            getline(ss, T.transaction_id, ',');
            getline(ss, T.timestamp, ',');
            getline(ss, T.sender_account, ',');
            getline(ss, T.receiver_account, ',');
            getline(ss, line, ',');        T.amount = line.empty()?0:stod(line);
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
            getline(ss, line, ',');        T.velocity_score    = line.empty()?0:stod(line);
            getline(ss, line, ',');        T.geo_anomaly_score = line.empty()?0:stod(line);
            getline(ss, T.payment_channel, ',');
            getline(ss, T.ip_address, ',');
            getline(ss, T.device_hash, ',');

            int ci = indexOf(T.payment_channel);
            if (ci >= 0) {
                channels[ci].push(T);
            }

            if (T.payment_channel == channel) {
                Node* nd = new Node(T);
                if (!head) head = tail = nd;
                else       tail->next = nd, tail = nd;
            }
            ++n;
        }

        cout<<"[LL] Loaded "<<n<<" rows | Payment-Channel: " << channel << " | Distribution: ";
        for (int i = 0; i < 4; ++i) {
            cout<< NAMES[i] << ":" << channels[i].count << " ";
        }
        cout<<"\n";
    }

    int size() const { return n; }

    //linear searches
    TransactionList getByPaymentChannel(int choice) const {
        int ci = choice - 1;
        if (ci < 0 || ci >= 4) throw runtime_error("Bad channel choice");
        return channels[ci];
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

    // binary searches
    TransactionList searchByTransactionTypeBinary(const string& key) const {
        TransactionList flat; flat.clear();
        for (Node* c = head; c; c = c->next) flat.push(c->d);
        stable_sort(flat.data, flat.data + flat.count,
                    [](auto &a, auto &b){ return a.transaction_type < b.transaction_type; });
        int lo=0, hi=flat.count;
        while (lo<hi) {
            int mid=(lo+hi)/2;
            if (flat.data[mid].transaction_type < key) lo=mid+1;
            else hi=mid;
        }
        TransactionList out; out.clear();
        while (lo<flat.count && flat.data[lo].transaction_type==key)
            out.push(flat.data[lo++]);
        return out;
    }

    TransactionList searchByLocationBinary(const string& key) const {
        TransactionList flat; flat.clear();
        for (Node* c = head; c; c = c->next) flat.push(c->d);
        stable_sort(flat.data, flat.data + flat.count,
                    [](auto &a, auto &b){ return a.location < b.location; });
        int lo=0, hi=flat.count;
        while (lo<hi) {
            int mid=(lo+hi)/2;
            if (flat.data[mid].location < key) lo=mid+1;
            else hi=mid;
        }
        TransactionList out; out.clear();
        while (lo<flat.count && flat.data[lo].location==key)
            out.push(flat.data[lo++]);
        return out;
    }

    void sortByLocation(bool asc=true) {
        head = quickSortList(head);
        if (!asc) reverseList();
        cout<<"[LL] Quick-Sorted Location ("<<(asc?"A-Z":"Z-A")<<")\n";
    }

    void sortByLocationMerge(bool asc=true) {
        head = mergeSortList(head);
        if (!asc) reverseList();
        cout<<"[LL] Merge-Sorted Location ("<<(asc?"A-Z":"Z-A")<<")\n";
    }

    void printFirstN(int k) const {
        int total=0;
        for (Node* c=head; c && total<k; c=c->next) ++total;
        int pages = (total + PAGE_SIZE - 1)/PAGE_SIZE;
        if (!pages) { cout<<"(no records)\n"; return; }

        int page=0;
        while (true) {
            int startIdx = page*PAGE_SIZE, endIdx = startIdx+PAGE_SIZE;
            cout<<"\n-- Displaying "<<total<<" Rows --\n";
            cout<<left
                <<setw(10)<<"ID"
                <<" | "<<setw(15)<<"Type"
                <<" | "<<setw(13)<<"Channel"
                <<" | "<<setw(12)<<"Location"
                <<" | "<<setw(10)<<"Amount"
                <<" | "<<setw(12)<<"Merchant"<<"\n";
            cout<<string(65,'-')<<"\n";

            Node* c=head; int i=0;
            while (c && i<endIdx) {
                if (i>=startIdx) {
                    auto& t = c->d;
                    cout<<setw(10)<<t.transaction_id
                        <<" | "<<setw(15)<<t.transaction_type
                        <<" | "<<setw(13)<<t.payment_channel
                        <<" | "<<setw(12)<<t.location
                        <<" | "<<setw(10)<<fixed<<setprecision(2)<<t.amount
                        <<" | "<<setw(12)<<t.merchant_category<<"\n";
                }
                c = c->next; ++i;
            }

            cout << "-- Page " << (page + 1) << " of " << pages << " --\n"
                << "Previous [1] | Next [2] | Back [3] | Jump to Page [4] | Export to JSON [5]\n"
                <<"Choose: ";
            int nav; cin>>nav; cin.ignore(numeric_limits<streamsize>::max(),'\n');
            if (nav==1 && page>0) --page;
            else if (nav==2 && page<pages-1) ++page;
            else if (nav==3) return;
            else if (nav==4) {
                cout<<"Page (1-"<<pages<<"): ";
                int tgt; cin>>tgt; cin.ignore(numeric_limits<streamsize>::max(),'\n');
                if (tgt>=1&&tgt<=pages) page = tgt-1;
            }
            else if(nav==5){
                cout<<"Enter JSON filename: ";
                string fn; getline(cin,fn);
                if(!fn.empty()){
                    string title=string("[LL] Split - Channel: ")+lastChannel;
                    exportToJSON(fn,title);
                    cout<<"Exported "<<total<<" rows to "<<fn<<"\n";
                }
            }
            else cout<<"...Invalid option.\n";
        }
    }

    void reset() {
        // 1) delete the linked list
        while (head) {
        Node* nxt = head->next;
        delete head;
        head = nxt;
        }
        tail = nullptr;
        n    = 0;
        lastChannel.clear();

        // 2) reset the per-channel caches
        for (int i = 0; i < 4; ++i) {
        channels[i] = TransactionList();
        }
    }

};

const char* LinkedListStore::NAMES[4] = {
    "card","ACH","UPI","wire_transfer"
};

// ------------------------------------------------------------------
// pagination + search dispatch
// ------------------------------------------------------------------
void handleSearch(bool useArr,
                  ArrayStore& arr,
                  LinkedListStore& ll,
                  bool useBinary) {
    const char* types[] = {"deposit","transfer","withdrawal","payment"};

    while (true) {
        cout << "\n-- SEARCH MENU --\n"
             << "  1) By Transaction Type\n"
             << "  2) By Location\n"
             << "  3) Back\n"
             << "Choose: ";
        int s;
        if (!(cin >> s)) { cin.clear(); cin.ignore(1e9, '\n'); continue; }
        cin.ignore(1e9, '\n');
        if (s == 3) break;

        TransactionList results;
        string          label, criterion;

        if (s == 1) {
            cout << "\nSelect type:\n";
            for (int i = 0; i < 4; ++i)
                cout << "  " << (i+1) << ") " << types[i] << "\n";
            cout << "Choose: ";
            int tt;
            if (!(cin >> tt) || tt < 1 || tt > 4) {
                cin.clear(); cin.ignore(1e9,'\n');
                continue;
            }
            cin.ignore(1e9,'\n');
            criterion = types[tt-1];
            label     = "Type=" + criterion;

            auto start = chrono::high_resolution_clock::now();
            size_t beforeRSS = getProcessRSS();
            if (useBinary) {
                results = useArr
                        ? arr.searchByTransactionTypeBinary(criterion)
                        : ll.searchByTransactionTypeBinary(criterion);
            } else {
                results = useArr
                        ? arr.getByTransactionType(criterion)
                        : ll.getByTransactionType(criterion);
            }
            auto stop    = chrono::high_resolution_clock::now();
            size_t afterRSS  = getProcessRSS();
            auto   dur       = chrono::duration_cast<chrono::milliseconds>(stop - start);
            size_t deltaRSS  = (afterRSS >= beforeRSS)
                            ? (afterRSS - beforeRSS)
                            : afterRSS;
            double beforeMB = double(afterRSS) / (1024.0 * 1024.0);
            double afterMB = double(beforeRSS) / (1024.0 * 1024.0);
            double deltaMB = double(deltaRSS) / (1024.0 * 1024.0);

            const char* prefix = useArr ? "[Array]" : "[Linked List]";
            cout << prefix << " Search Transaction - Time Used: " << dur.count() << " ms\n"
                << prefix << " Search Transaction - RSS Before: " << beforeMB << " MB (" << beforeRSS  << " bytes)\n"
                << prefix << " Search Transaction - RSS After: " << afterMB << " MB (" << afterRSS  << " bytes)\n"
                << prefix << " Search Transaction - Memory Used: " << deltaMB << " MB (" << deltaRSS  << " bytes)\n";
        }
        else if (s == 2) {
            cout << "Enter location: ";
            getline(cin, criterion);
            label = "Location=" + criterion;

            auto start = chrono::high_resolution_clock::now();
            size_t beforeRSS = getProcessRSS();
            if (useBinary) {
                results = useArr
                        ? arr.searchByLocationBinary(criterion)
                        : ll.searchByLocationBinary(criterion);
            } else {
                results = useArr
                        ? arr.getByLocation(criterion)
                        : ll.getByLocation(criterion);
            }
            auto stop    = chrono::high_resolution_clock::now();
            size_t afterRSS  = getProcessRSS();
            auto   dur       = chrono::duration_cast<chrono::milliseconds>(stop - start);
            size_t deltaRSS  = (afterRSS >= beforeRSS)
                            ? (afterRSS - beforeRSS)
                            : afterRSS;
            double beforeMB = double(afterRSS) / (1024.0 * 1024.0);
            double afterMB = double(beforeRSS) / (1024.0 * 1024.0);
            double deltaMB = double(deltaRSS) / (1024.0 * 1024.0);

            const char* prefix = useArr ? "[Array]" : "[Linked List]";
            cout << prefix << " Search Location - Time Used: " << dur.count() << " ms\n"
                << prefix << " Search Location - RSS Before: " << beforeMB << " MB (" << beforeRSS  << " bytes)\n"
                << prefix << " Search Location - RSS After: " << afterMB << " MB (" << afterRSS  << " bytes)\n"
                << prefix << " Search Location - Memory Used: " << deltaMB << " MB (" << deltaRSS  << " bytes)\n";
        }
        else {
            cout << "Invalid choice.\n";
            continue;
        }

        // Capture these results for export later
        lastResults = std::move(results);
        lastLabel   = label;
        hasResults  = true;

        // Paginate *lastResults*
        int total = lastResults.count;
        int pages = (total + PAGE_SIZE - 1) / PAGE_SIZE;
        if (!pages) { cout << "(no records)\n"; continue; }

        int page = 0;
        while (true) {
            int startIdx = page * PAGE_SIZE;
            int endIdx   = min(startIdx + PAGE_SIZE, total);

            cout << "\n-- " << lastLabel << " --\n";
            cout << left
                 << setw(10) << "ID"
                 << " | " << setw(13) << "Channel"
                 << " | " << setw(13) << "Type"
                 << " | " << setw(12) << "Location"
                 << " | " << setw(10) << "Amount"
                 << " | " << setw(12) << "Merchant" << "\n";
            cout << string(80, '-') << "\n";

            for (int i = startIdx; i < endIdx; ++i) {
                const auto& t = lastResults.data[i];
                cout << setw(10) << t.transaction_id
                     << " | " << setw(13) << t.payment_channel
                     << " | " << setw(13) << t.transaction_type
                     << " | " << setw(12) << t.location
                     << " | " << setw(10) << fixed << setprecision(2) << t.amount
                     << " | " << setw(12) << t.merchant_category << "\n";
            }

            cout << "-- Page " << (page + 1) << " of " << pages << " --\n"
                 << "Prev [1] | Next [2] | Back [3] | Jump [4]\n"
                 << "Choose: ";
            int nav;
            if (!(cin >> nav)) {
                cin.clear(); cin.ignore(numeric_limits<streamsize>::max(), '\n');
                cout << "...Invalid input.\n";
                continue;
            }
            cin.ignore(numeric_limits<streamsize>::max(), '\n');

            if (nav == 1 && page > 0) --page;
            else if (nav == 2 && page < pages - 1) ++page;
            else if (nav == 3) break;
            else if (nav == 4) {
                cout << "Page (1-" << pages << "): ";
                int tgt;
                if (!(cin >> tgt) || tgt < 1 || tgt > pages) {
                    cin.clear(); cin.ignore(numeric_limits<streamsize>::max(), '\n');
                    cout << "...Invalid page number.\n";
                } else {
                    page = tgt - 1;
                }
                cin.ignore(numeric_limits<streamsize>::max(), '\n');
            } else {
                cout << "...Invalid option.\n";
            }
        }
    }
}

int main() {
    ArrayStore arr, fullArr;
    LinkedListStore ll, fullLL;

    const char* channels[] = {"card","ACH","UPI","wire_transfer"};
    bool channelLoaded = false;
    int channelChoice = 0;

    while (true) {
        cout << "\n==== PICK DS ====\n"
             << "1) Array-based\n"
             << "2) Linked-list\n"
             << "3) Exit\n"
             << "Choose: ";
        int ds;
        while (!(cin >> ds) || ds < 1 || ds > 3) {
            cin.clear(); cin.ignore(numeric_limits<streamsize>::max(), '\n');
            cout << "...Please enter 1, 2, or 3.\n";
        }
        cin.ignore(numeric_limits<streamsize>::max(), '\n');
        if (ds == 3) break;
        bool useArr = (ds == 1);

        // Load full dataset
        if (useArr)
            fullArr.loadAllFromCSV("financial_fraud_detection_dataset.csv");
        else
            fullLL.loadAllFromCSV("financial_fraud_detection_dataset.csv");

        channelLoaded = false;
        channelChoice = 0;

        while (true) {
            cout << "\n==== FEATURES ====\n"
                 << "1) Split by Payment Channel\n"
                 << "2) Search (type/location)\n"
                 << "3) Sort by Location\n"
                 << "4) Display Data (All)\n"
                 << "5) Export Search Results to JSON\n"
                 << "6) Back\n"
                 << "7) Exit\n"
                 << "Choose: ";
            int cmd;
            while (!(cin >> cmd) || cmd < 1 || cmd > 7) {
                cin.clear(); cin.ignore(numeric_limits<streamsize>::max(), '\n');
                cout << "...Please enter a number 1-7.\n";
            }
            cin.ignore(numeric_limits<streamsize>::max(), '\n');

            if (cmd == 6) break;
            if (cmd == 7) return 0;

            switch (cmd) {
            case 1: {  // Split by Payment Channel
                int pc;
                do {
                    cout << "\nSelect channel:\n";
                    for (int i = 0; i < 4; ++i)
                        cout << "  " << (i+1) << ") " << channels[i] << "\n";
                    cout << "Choose: ";
                } while (!(cin >> pc) || pc < 1 || pc > 4);
                cin.ignore(numeric_limits<streamsize>::max(), '\n');
                
                if (useArr) arr.reset();
                else ll.reset();
                auto start = chrono::high_resolution_clock::now();
                size_t beforeRSS = getProcessRSS();
                // MemoryTracker::reset();

                // channelLoaded = true;
                // channelChoice = pc;
                if (useArr)
                    arr.loadFromCSV("financial_fraud_detection_dataset.csv", channels[pc-1]);
                else
                    ll.loadFromCSV("financial_fraud_detection_dataset.csv", channels[pc-1]);

                auto stop    = chrono::high_resolution_clock::now();
                size_t afterRSS  = getProcessRSS();
                // size_t peak = MemoryTracker::getPeak();
                auto   dur       = chrono::duration_cast<chrono::milliseconds>(stop - start);
                
                // const char* prefix = useArr ? "[Array]" : "[Linked List]";
                // cout << prefix << " Split - Time Used: "  << " ms\n"
                //     << prefix << " Split - Memory Used: "
                //     << peak   << " bytes (" << (peak/1024.0/1024.0) << " MB)\n";
                size_t deltaRSS  = (afterRSS >= beforeRSS)
                                ? (afterRSS - beforeRSS)
                                : afterRSS;

                double beforeMB = double(afterRSS) / (1024.0 * 1024.0);
                double afterMB = double(beforeRSS) / (1024.0 * 1024.0);
                double deltaMB = double(deltaRSS) / (1024.0 * 1024.0);

                const char* prefix = useArr ? "[Array]" : "[Linked List]";
                cout << prefix << " Split - Time Used: " << dur.count() << " ms\n"
                    << prefix << " Split - RSS Before: " << beforeMB << " MB (" << beforeRSS  << " bytes)\n"
                    << prefix << " Split - RSS After: " << afterMB << " MB (" << afterRSS  << " bytes)\n"
                    << prefix << " Split - Memory Used: " << deltaMB << " MB (" << deltaRSS  << " bytes)\n";

                if (useArr)      arr.printFirstN(MAX_TRANSACTIONS);
                else             ll.printFirstN(MAX_TRANSACTIONS);
                break;
            }
            case 2: {  // Search on full dataset
                int alg;
                do {
                    cout << "\nSelect search algorithm:\n"
                         << "  1) Linear\n"
                         << "  2) Binary\n"
                         << "Choose: ";
                } while (!(cin >> alg) || alg < 1 || alg > 2);
                cin.ignore(numeric_limits<streamsize>::max(), '\n');

                handleSearch(useArr, fullArr, fullLL, alg == 2);
                break;
            }
            case 3: {  // Sort on full dataset
                int sa;
                do {
                    cout << "\nChoose sorting algorithm:\n"
                         << "  1) Quick Sort\n"
                         << "  2) Merge Sort\n"
                         << "Choose: ";
                } while (!(cin >> sa) || (sa != 1 && sa != 2));
                cin.ignore(numeric_limits<streamsize>::max(), '\n');

                int d;
                do {
                    cout << "  1) A-Z\n"
                         << "  2) Z-A\n"
                         << "Choose: ";
                } while (!(cin >> d) || (d != 1 && d != 2));
                cin.ignore(numeric_limits<streamsize>::max(), '\n');

                bool asc = (d == 1);
                auto start = chrono::high_resolution_clock::now();
                size_t beforeRSS = getProcessRSS();
                if (useArr) {
                    if (sa == 1)      fullArr.sortByLocation(asc);
                    else              fullArr.sortByLocationMerge(asc);
                } else {
                    if (sa == 1)      fullLL.sortByLocation(asc);
                    else              fullLL.sortByLocationMerge(asc);
                }
                auto stop    = chrono::high_resolution_clock::now();
                size_t afterRSS  = getProcessRSS();
                auto   dur       = chrono::duration_cast<chrono::milliseconds>(stop - start);
                size_t deltaRSS  = (afterRSS >= beforeRSS)
                                ? (afterRSS - beforeRSS)
                                : afterRSS;

                double beforeMB = double(afterRSS) / (1024.0 * 1024.0);
                double afterMB = double(beforeRSS) / (1024.0 * 1024.0);
                double deltaMB = double(deltaRSS) / (1024.0 * 1024.0);

                const char* prefix = useArr ? "[Array]" : "[Linked List]";
                const char* algName = (sa == 1) ? "QuickSort" : "MergeSort";
                cout << prefix << algName << " - Time Used: " << dur.count() << " ms\n"
                    << prefix << algName << " - RSS Before: " << beforeMB << " MB (" << beforeRSS  << " bytes)\n"
                    << prefix << algName << " - RSS After: " << afterMB << " MB (" << afterRSS  << " bytes)\n"
                    << prefix << algName << " - Memory Used: " << deltaMB << " MB (" << deltaRSS  << " bytes)\n";
                break;
            }
            case 4: {  // Display Data 
                if (useArr)      fullArr.printFirstN(MAX_TRANSACTIONS);
                else             fullLL.printFirstN(MAX_TRANSACTIONS);
                break;
            }
            case 5: {  // Export
                        if (!hasResults) {
                            cout << "No search results to export.\n";
                            break;
                        }
                        cout << "Enter JSON filename: ";
                        string fn; getline(cin, fn);
                        if (fn.empty()) break;
                        string store = useArr ? "[Array]" : "[Linked List]";
                        string title = store + " Search - " + lastLabel;
                        lastResults.exportToJSON(fn, title);
                        cout << "Exported " << lastResults.count << " rows to " << fn << "\n";
                        break;
                   }      
            default:
                cout << "Invalid option.\n";
            }
        }
    }

    cout << "Program terminated.\n";
    return 0;
}
