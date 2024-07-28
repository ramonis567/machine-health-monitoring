#include <iostream>
#include <cstdlib>
#include <chrono>
#include <ctime>
#include <thread>
#include <unistd.h>
#include "json.hpp" // json handling
#include "mqtt/client.h" // paho mqtt
#include <iomanip>
#include <string.h>
#include <fstream>
#include <sys/sysinfo.h>
#include <sstream>
#include <thread>
#include <mutex>
//#include <comdef.h>
//#include <Wbemidl.h>

using namespace std;

#define SENSOR_I 1;     //sensor topic update interval
#define MONITOR_I 30;   //monitor topic update interval
#define CPU_USAGE_ID "cpu_usage";
#define RAM_USAGE_ID "ram_usage";

#define QOS 1
#define BROKER_ADDRESS "tcp://localhost:1883"

std::mutex m;

std::string monitor_clientId = "sensor-monitor";
mqtt::client m_client(BROKER_ADDRESS, monitor_clientId);

string getMachineId(){
    // Get the unique machine identifier, in this case, the hostname.
    char hostname[1024];
    gethostname(hostname, 1024);
    std::string machineId(hostname);
    return std::string(hostname);
}
std::string machineId = getMachineId();


float stringToFloat(const std::string &str){        //String to float converter function
    float val;
    std::stringstream ss(str);
    ss >> val;
    return val;

}

void publish_msg(mqtt::message msg){
    m_client.publish(msg);
}

float getCpuUsage(){
    //CPU temperature info

    std::ifstream proc_stat("/proc/stat");
    if(!proc_stat.is_open()){
        std::cerr << "Error opening /proc/stat" << std::endl;
        return -1.0; // Return a negative value to indicate an error
    }
    proc_stat.ignore(5, ' '); 
    std::vector<size_t> times;
    for (size_t time; proc_stat >> time; times.push_back(time));
    const std::vector<size_t> cpu_times = times;
    if (cpu_times.size() < 4)
        return false;
    double idle_time = cpu_times[3];
    double total_time = std::accumulate(cpu_times.begin(), cpu_times.end(), 0);
    //clog << idle_time << endl;
    //clog << total_time << endl;
    float aux = 1.0-(idle_time/total_time);
    aux *= 100.0;
    //clog << aux << endl;
    return aux;
}

float getRamUsage(){
    //RAM usage info
    std::ifstream meminfo("/proc/meminfo");
    if (!meminfo.is_open()) {
        std::cerr << "Error opening /proc/meminfo" << std::endl;
        return -1.0; // Return a negative value to indicate an error
    }

    string line;
    float m_total = 0;
    float m_avail = 0;
    while(getline(meminfo, line)){
        if (line.find("MemTotal:") == 0)
            m_total = stringToFloat(line.substr(10));
        else if(line.find("MemAvailable:") == 0)
            m_avail = stringToFloat(line.substr(14));
    }
    meminfo.close();

    return ((m_total - m_avail)/m_total)*100.0;
}

string getTimestamp(){
    // Get the current time in ISO 8601 format.
    auto now = std::chrono::system_clock::now();
    std::time_t now_c = std::chrono::system_clock::to_time_t(now);
    std::tm* now_tm = std::localtime(&now_c);
    std::stringstream ss;
    ss << std::put_time(now_tm, "%FT%TZ");
    std::string timestamp = ss.str();

    return timestamp;
}

void cpuUsage_run(){
    
    nlohmann::json j;
    float cpu_u = getCpuUsage();
    if(cpu_u == -1.0)
        return;
    j["timestamp"] = getTimestamp();
    j["value"] = cpu_u;

    // Publish the JSON message to the appropriate topic.
    std::string topic = "/sensors/" + machineId + "/cpu_usage";
    mqtt::message msg(topic, j.dump(), QOS, false);
    publish_msg(msg);
    std::clog << "message published - topic: " << topic << " - message: " << j.dump() << std::endl;

    
}

void ramUsage_run(){
    
    nlohmann::json j;
    float ram_u = getRamUsage();
    if(ram_u == -1.0)
        return;
    j["timestamp"] = getTimestamp();
    j["value"] = getRamUsage();

    // Publish the JSON message to the appropriate topic.
    std::string topic = "/sensors/" + machineId + "/ram_usage";
    mqtt::message msg(topic, j.dump(), QOS, false);
    publish_msg(msg);
    std::clog << "message published - topic: " << topic << " - message: " << j.dump() << std::endl;
        
}

void monitor_run(){
    nlohmann::json j;
    j["machine_id"] = machineId;
    
    nlohmann::json sensor_info;
    sensor_info["sensor_id"] = CPU_USAGE_ID;
    sensor_info["data_type"] = "float";
    sensor_info["data_interval"] = SENSOR_I;
    j["sensors"].push_back(sensor_info);
    
    sensor_info["sensor_id"] = RAM_USAGE_ID;
    sensor_info["data_type"] = "float";
    sensor_info["data_interval"] = SENSOR_I;
    j["sensors"].push_back(sensor_info);  

    // Publish the JSON message to the appropriate topic.
    std::string topic = "/sensors";
    mqtt::message msg(topic, j.dump(), QOS, false);
    publish_msg(msg);
    std::clog << "message published - topic: " << topic << " - message: " << j.dump() << std::endl;
    
}

int main(int argc, char* argv[]) {

    // Connect to the MQTT broker.
    mqtt::connect_options connOpts;
    connOpts.set_keep_alive_interval(20);
    connOpts.set_clean_session(true);

    try {
        m_client.connect(connOpts);
    } catch (mqtt::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }
    std::clog << "connected to the broker" << std::endl;

    clog << "MachineID: " << machineId << endl;

    int mf = MONITOR_I;
    int sf = SENSOR_I;

    monitor_run();

    while(true){
        cpuUsage_run();
        ramUsage_run();
        std::this_thread::sleep_for(std::chrono::seconds(sf));
    }
    

    //std::thread (monitor_thread, mf).detach();
    //std::thread (cpuUsage_thread, sf).detach();
    //std::thread (ramUsage_thread, sf).detach();

    return EXIT_SUCCESS;
}
