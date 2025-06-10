#ifndef TRANSACTION_HPP
#define TRANSACTION_HPP

#include <string>

struct Transaction {
    std::string transaction_id;
    std::string timestamp;
    std::string sender_account;
    std::string receiver_account;
    double amount = 0.0;
    std::string transaction_type;
    std::string merchant_category;
    std::string location;
    std::string device_used;
    bool is_fraud = false;
    std::string fraud_type;
    std::string time_since_last_transaction;
    std::string spending_deviation_score;
    double velocity_score = 0.0;
    double geo_anomaly_score = 0.0;
    std::string payment_channel;
    std::string ip_address;
    std::string device_hash;
};

#endif
