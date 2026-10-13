#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include <functional>
#include <map>
#include <stdexcept>
#include <cstring>
#include <json.hpp>

namespace cms {
using Json = nlohmann::json;
using Bytes = std::vector<uint8_t>;
constexpr const char* journal_name = "cms_l2_raw_v1";
constexpr int16_t journal_type = 22001;
constexpr size_t envelope_size = 96;
enum Kind : uint16_t { Data=1, Start=2, Progress=3, End=4 };
enum Flag : uint8_t { Truncated=1, Malformed=2, Incomplete=4 };
template<class T> T read_le(const uint8_t* p) {
    using U=typename std::make_unsigned<T>::type;
    U v=0; for(size_t i=0;i<sizeof(T);++i) v|=U(p[i])<<(8*i);
    T result; std::memcpy(&result,&v,sizeof(T)); return result;
}
template<class T> void put_le(uint8_t* p,T v) {
    using U=typename std::make_unsigned<T>::type;
    U u=static_cast<U>(v); for(size_t i=0;i<sizeof(T);++i) p[i]=uint8_t(u>>(8*i));
}
struct Record {
    Kind kind=Data; uint8_t market=0,flags=0; uint32_t day=0;
    uint64_t epoch=0,seq=0,recv_seq=0; int64_t recv_ns=0;
    uint32_t src_ip=0,dst_ip=0; uint16_t src_port=0,dst_port=0;
    uint32_t ifindex=0,kernel_drops=0; uint64_t aux1=0,aux2=0;
    Bytes payload;
};
Bytes encode(const Record& r);
Record decode(const uint8_t* p,size_t n);
struct Event {
    uint8_t market=0,type=0; char action=0; uint32_t feed_seq=0;
    uint16_t channel=0; uint64_t business_seq=0,buy=0,sell=0;
    uint64_t time=0,quantity=0,value=0; uint32_t price=0;
    uint8_t side=0; std::string symbol;
};
void parse_datagram(uint8_t market,const uint8_t* p,size_t n,
                    const std::function<void(const Event&)>& visit);
Json event_json(const Event& e);
int64_t wall_ns();
uint32_t beijing_day();
uint32_t ipv4(const std::string& s);
std::string ipv4_text(uint32_t v);
void require(bool condition,const std::string& message);
class Args {
    std::map<std::string,std::string> values;
public:
    Args(int argc,char** argv);
    std::string get(const std::string& key,const std::string& fallback="") const;
    int64_t number(const std::string& key,int64_t fallback) const;
    bool has(const std::string& key) const;
};
class Fd {
    int value_=-1;
public:
    explicit Fd(int v=-1):value_(v){}
    ~Fd(); Fd(const Fd&)=delete; Fd& operator=(const Fd&)=delete;
    Fd(Fd&& x) noexcept:value_(x.value_){x.value_=-1;}
    int get()const{return value_;}
};
Fd lock_file(const std::string& path);
void fresh_journal(const std::string& path);
void require_runtime();
void send_all(int fd,const void* p,size_t n);
void recv_all(int fd,void* p,size_t n);
void send_blob(int fd,const Bytes& b);
Bytes recv_blob(int fd);
void write_json(const std::string& path,const Json& j);
// Sequence is continuous across sessions within one trading-day stream.
// END closes a session, not the entire day's journal. A new START without a
// preceding END explicitly represents an unclean interruption.
struct StreamState {
    uint64_t seq=0,epoch=0,sessions=0,unclean_restarts=0;
    uint32_t day=0; bool ended=false;
    void observe(const Record& r);
};
struct JournalTail { Record last; bool found=false; };
// Validate page inventory/headers and scan at most the last nonempty page.
// Call only while holding writer.lock, before creating a Kungfu writer.
JournalTail journal_tail(const std::string& path);
// Checks frame and page boundaries, hash, envelope sequence, and explicit END.
void read_journal(const std::string& path,
                  const std::function<void(const Record&,int64_t,const Bytes&)>& visit,
                  bool require_end=true);
// 页级并行读取接口（离线工具用；语义与 read_journal 相同，编排层按页 fan-out、
// 按页号归并）。online 实时路径不使用。
using JournalPages=std::vector<std::pair<int,std::string>>;
JournalPages inventory_pages(const std::string& path);
void scan_page(const std::pair<int,std::string>& page,bool intermediate,
               const std::function<void(const Record&,int64_t,const Bytes&)>& visit);
}
