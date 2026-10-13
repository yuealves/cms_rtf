#include "cms/metrics.hpp"
#include <ctime>
#include <iomanip>
#include <sstream>
#include <utility>

namespace cms {
namespace {
constexpr int64_t billion=1000000000;
std::optional<int64_t> midnight(uint32_t day) {
    // Calendar conversion happens only when the date changes, not on each tick.
    static thread_local uint32_t cached_day=0;
    static thread_local std::optional<int64_t> cached;
    if(day==cached_day)return cached;
    cached_day=day;cached.reset();
    int year=day/10000,month=day/100%100,date=day%100;
    if(year<2000||year>2100||month<1||month>12||date<1||date>31)return cached;
    std::tm tm{};tm.tm_year=year-1900;tm.tm_mon=month-1;tm.tm_mday=date;
    auto utc=timegm(&tm);
    if(tm.tm_year!=year-1900||tm.tm_mon!=month-1||tm.tm_mday!=date)return cached;
    cached=(int64_t(utc)-8*3600)*billion;return cached;
}
}
std::optional<int64_t> exchange_ns(const Event&e,uint32_t day) {
    uint64_t clock=e.time;int millisecond;
    if(e.market==1){millisecond=int(clock%100)*10;clock/=100;}
    else if(e.market==2){day=uint32_t(clock/1000000000ULL);clock%=1000000000ULL;
        millisecond=int(clock%1000);clock/=1000;}
    else return {};
    int sec=clock%100,minute=clock/100%100;uint64_t hour=clock/10000;
    if(hour>23||minute>59||sec>59)return {};
    auto start=midnight(day);if(!start)return {};
    return *start+(int64_t(hour)*3600+minute*60+sec)*billion+int64_t(millisecond)*1000000;
}
SecondStats::SecondStats(int64_t now,std::function<void(const std::string&)> output)
    :second(now/billion),sink(std::move(output)){}
void SecondStats::emit(bool final,const std::string&quality) {
    std::time_t local=second;std::tm tm{};localtime_r(&local,&tm);char date[32];
    strftime(date,sizeof(date),"%Y-%m-%dT%H:%M:%S",&tm);
    std::ostringstream out;out<<date<<" partial="<<(partial||final?1:0);
    constexpr const char* names[]={"SH_order","SH_trade","SZ_order","SZ_trade"};
    for(size_t i=0;i<cells.size();++i){const auto&c=cells[i];
        out<<' '<<names[i]<<"="<<c.count<<'/';
        if(c.last)out<<std::fixed<<std::setprecision(3)<<double(*c.last)/1000000;else out<<'-';
        if(c.invalid_time)out<<" invalid_time_"<<names[i]<<'='<<c.invalid_time;
    }
    out<<" clock_steps="<<clock_steps<<' '<<quality<<'\n';sink(out.str());
}
void SecondStats::advance(int64_t now,const std::string&quality) {
    int64_t next=now/billion;
    if(next<second){++clock_steps;return;}
    // Bound log work even if the wall clock jumps by hours.
    if(next-second>60){++clock_steps;emit(true,quality);cells={};second=next;partial=true;return;}
    while(next>second){emit(false,quality);cells={};++second;partial=false;}
}
void SecondStats::observe(const Event&e,uint32_t day,int64_t receive_ns) {
    if(e.market<1||e.market>2||(e.action!='A'&&e.action!='D'&&e.action!='T'))return;
    auto&cell=cells[(e.market-1)*2+(e.action=='T'?1:0)];++cell.count;
    auto timestamp=exchange_ns(e,day);
    if(timestamp)cell.last=receive_ns-*timestamp;
    else{cell.last.reset();++cell.invalid_time;}
}
void SecondStats::finish(const std::string&quality){emit(true,quality);}
}
