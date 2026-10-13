#include "cms/core.hpp"
#include <chrono>
#include <fstream>
#include <filesystem>
#include <cerrno>
#include <fcntl.h>
#include <unistd.h>
#include <sys/file.h>
#include <sys/socket.h>
#include <arpa/inet.h>
namespace fs=std::filesystem;
namespace cms {
void require(bool ok,const std::string&s){if(!ok)throw std::runtime_error(s);}
int64_t wall_ns(){return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();}
uint32_t beijing_day(){time_t t=wall_ns()/1000000000+8*3600;tm b{};gmtime_r(&t,&b);return (b.tm_year+1900)*10000+(b.tm_mon+1)*100+b.tm_mday;}
uint32_t ipv4(const std::string&s){in_addr a{};require(inet_pton(AF_INET,s.c_str(),&a)==1,"invalid IPv4: "+s);return ntohl(a.s_addr);}
std::string ipv4_text(uint32_t v){in_addr a{};a.s_addr=htonl(v);char out[INET_ADDRSTRLEN];inet_ntop(AF_INET,&a,out,sizeof(out));return out;}
Args::Args(int argc,char**argv){for(int i=1;i<argc;++i){std::string k=argv[i];require(k.rfind("--",0)==0,"expected --option");require(i+1<argc,"missing value: "+k);require(!values.count(k.substr(2)),"duplicate argument: "+k);values[k.substr(2)]=argv[++i];}}
std::string Args::get(const std::string&k,const std::string&v)const{auto it=values.find(k);return it==values.end()?v:it->second;}
int64_t Args::number(const std::string&k,int64_t v)const{auto s=get(k);if(s.empty())return v;size_t used=0;auto n=std::stoll(s,&used);require(used==s.size(),"invalid integer: "+k);return n;}
bool Args::has(const std::string&k)const{return values.count(k);}
Fd::~Fd(){if(value_>=0)::close(value_);}
Fd lock_file(const std::string&p){Fd fd(::open(p.c_str(),O_CREAT|O_RDWR|O_CLOEXEC,0600));require(fd.get()>=0,"open lock failed: "+p);require(flock(fd.get(),LOCK_EX|LOCK_NB)==0,"already running/locked: "+p);return fd;}
void fresh_journal(const std::string&p){
    require(!p.empty() && p[0]=='/' && p.size()<95,"journal path must be absolute and <95 bytes (Kungfu ABI)");
    fs::create_directories(p);
    for(const auto&entry:fs::directory_iterator(p))
        require(entry.path().extension()!=".journal","refusing existing journal; use new attempt");
}
void require_runtime(){
    const char*d=getenv("KUNGFU_DATA_DIR");require(d&&d[0]=='/'&&std::strlen(d)<70,"set absolute KUNGFU_DATA_DIR <70 bytes");
    require(fs::exists(std::string(d)+"/socket/paged.sock"),"private paged socket missing");
}
void send_all(int fd,const void*p,size_t n){auto*b=static_cast<const uint8_t*>(p);while(n){ssize_t z=send(fd,b,n,MSG_NOSIGNAL);if(z<0&&errno==EINTR)continue;require(z>0,"control send failed");b+=z;n-=z;}}
void recv_all(int fd,void*p,size_t n){auto*b=static_cast<uint8_t*>(p);while(n){ssize_t z=recv(fd,b,n,0);if(z<0&&errno==EINTR)continue;require(z>0,"control closed/timed out");b+=z;n-=z;}}
void send_blob(int fd,const Bytes&b){require(b.size()<=100000,"control frame too large");uint8_t h[4];put_le<uint32_t>(h,b.size());send_all(fd,h,4);send_all(fd,b.data(),b.size());}
Bytes recv_blob(int fd){uint8_t h[4];recv_all(fd,h,4);auto n=read_le<uint32_t>(h);require(n<=100000,"control frame too large");Bytes b(n);recv_all(fd,b.data(),n);return b;}
void write_json(const std::string&p,const Json&j){std::ofstream out(p,std::ios::trunc);require(bool(out),"cannot write "+p);out<<j.dump(2)<<'\n';out.close();require(bool(out),"write failed "+p);}
}
