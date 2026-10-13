#include "cms/core.hpp"
#include <limits>
namespace cms {
Bytes encode(const Record& r) {
    if(r.payload.size()>65535)throw std::runtime_error("record payload too large");
    Bytes b(envelope_size+r.payload.size(),0); auto*p=b.data();
    std::memcpy(p,"CMS1",4); put_le<uint16_t>(p+4,1); put_le<uint16_t>(p+6,envelope_size);
    put_le<uint16_t>(p+8,r.kind); p[10]=r.market;p[11]=r.flags;
    put_le(p+12,r.day);put_le(p+16,r.epoch);put_le(p+24,r.seq);put_le(p+32,r.recv_seq);
    put_le(p+40,r.recv_ns);put_le(p+48,r.src_ip);put_le(p+52,r.dst_ip);
    put_le(p+56,r.src_port);put_le(p+58,r.dst_port);put_le(p+60,r.ifindex);
    put_le(p+64,r.kernel_drops);put_le<uint32_t>(p+68,r.payload.size());
    put_le(p+72,r.aux1);put_le(p+80,r.aux2);
    std::copy(r.payload.begin(),r.payload.end(),b.begin()+envelope_size);return b;
}
Record decode(const uint8_t*p,size_t n) {
    require(n>=envelope_size,"short CMS envelope");
    require(std::memcmp(p,"CMS1",4)==0 && read_le<uint16_t>(p+4)==1 &&
            read_le<uint16_t>(p+6)==envelope_size,"unsupported CMS envelope");
    require(read_le<uint32_t>(p+68)==n-envelope_size && n<=envelope_size+65535,"invalid CMS payload length");
    Record r;r.kind=Kind(read_le<uint16_t>(p+8));
    require(r.kind>=Data && r.kind<=End,"unknown record kind");
    r.market=p[10];r.flags=p[11];r.day=read_le<uint32_t>(p+12);
    r.epoch=read_le<uint64_t>(p+16);r.seq=read_le<uint64_t>(p+24);r.recv_seq=read_le<uint64_t>(p+32);
    r.recv_ns=read_le<int64_t>(p+40);r.src_ip=read_le<uint32_t>(p+48);r.dst_ip=read_le<uint32_t>(p+52);
    r.src_port=read_le<uint16_t>(p+56);r.dst_port=read_le<uint16_t>(p+58);
    r.ifindex=read_le<uint32_t>(p+60);r.kernel_drops=read_le<uint32_t>(p+64);
    r.aux1=read_le<uint64_t>(p+72);r.aux2=read_le<uint64_t>(p+80);
    require(r.kind!=Data || r.market==1 || r.market==2,"unknown market");
    r.payload.assign(p+envelope_size,p+n);return r;
}
void parse_datagram(uint8_t market,const uint8_t*p,size_t n,const std::function<void(const Event&)>& visit) {
    require(market==1 || market==2,"invalid market"); require(n>0,"empty datagram");
    size_t off=0;
    while(off<n) {
        require(n-off>=9,"short EFH header"); const auto*q=p+off; uint8_t type=q[8];
        Event e; e.market=market;e.type=type;e.feed_seq=read_le<uint32_t>(q);
        if(type==(market==1?162:123)) {
            require(n-off>=16,"short heartbeat"); e.action='H';visit(e);off+=16;continue;
        }
        require(n-off>=72,"short EFH tick");
        if(market==1) {
            require(type==62,"unexpected SH message type (snapshot is not subscribed)");
            e.business_seq=read_le<uint64_t>(q+9);e.channel=read_le<uint16_t>(q+17);
            e.symbol=std::string(reinterpret_cast<const char*>(q+21),strnlen(reinterpret_cast<const char*>(q+21),9));
            e.time=read_le<uint32_t>(q+30); e.action=q[34];
            require(e.action=='A'||e.action=='D'||e.action=='T'||e.action=='S',"unknown SH event");
            e.buy=read_le<uint64_t>(q+35);e.sell=read_le<uint64_t>(q+43);
            e.price=read_le<uint32_t>(q+51);e.quantity=read_le<uint64_t>(q+55);
            e.value=read_le<uint64_t>(q+63);e.side=q[71];
            require(e.action=='S'||e.side<=(e.action=='T'?2:1),"invalid SH side");
            require(e.action=='S'||e.quantity%1000==0,"fractional SH stock quantity");
        } else {
            require(type==23||type==24,"unexpected SZ message type (snapshot is not subscribed)");
            e.symbol=std::string(reinterpret_cast<const char*>(q+11),strnlen(reinterpret_cast<const char*>(q+11),9));
            e.time=read_le<uint64_t>(q+21);e.channel=read_le<uint16_t>(q+29);
            require(read_le<int64_t>(q+31)>=0,"negative SZ sequence");
            e.business_seq=read_le<uint64_t>(q+31);
            if(type==23) {
                e.action='A';e.price=read_le<uint32_t>(q+43);
                require(read_le<int64_t>(q+47)>=0,"negative SZ quantity");
                e.quantity=read_le<uint64_t>(q+47);e.side=q[55];e.value=q[56];
                require(e.side=='1'||e.side=='2',"invalid SZ order side");
                require(e.value=='1'||e.value=='2'||e.value=='U',"invalid SZ order type");
                if(e.side=='1')e.buy=e.business_seq;else e.sell=e.business_seq;
            } else {
                require(read_le<int64_t>(q+43)>=0 && read_le<int64_t>(q+51)>=0,"negative SZ order reference");
                e.buy=read_le<uint64_t>(q+43);e.sell=read_le<uint64_t>(q+51);
                e.price=read_le<uint32_t>(q+59);
                require(read_le<int64_t>(q+63)>=0,"negative SZ trade quantity");
                e.quantity=read_le<uint64_t>(q+63);
                require(q[71]=='F'||q[71]=='4',"invalid SZ execution type");
                e.action=q[71]=='F'?'T':'D';
            }
            require(e.quantity%100==0,"fractional SZ stock quantity");
        }
        visit(e);off+=72;
    }
}
Json event_json(const Event&e) {
    Json j={{"market",e.market==1?"SH":"SZ"},{"message_type",e.type},
      {"action",std::string(1,e.action)},{"feed_seq",e.feed_seq}};
    if(e.action=='H')return j;
    j["symbol"]=e.symbol;j["channel"]=e.channel;j["business_seq"]=e.business_seq;
    j["buy_ref"]=e.buy;j["sell_ref"]=e.sell;j["exchange_time_raw"]=e.time;
    j["price_raw"]=e.price;j["quantity_raw"]=e.quantity;j["side_raw"]=e.side;
    j["price_scale"]=e.market==1?1000:10000;j["quantity_scale"]=e.market==1?1000:100;
    if(e.market==1){j["trade_value_raw"]=e.value;
       j["trade_value_meaning"]=e.action=='A'?"already_traded_qty_x1000":e.action=='T'?"amount_x100000":"not_applicable";}
    else if(e.type==23)j["order_type"]=std::string(1,char(e.value));
    return j;
}
}
