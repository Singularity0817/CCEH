// MIT License

// Copyright (c) [2020] [Xingsheng Zhao]

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

#pragma once

#include <sstream>
#include <thread>
#include <vector>
struct IPMInfo{
    std::string dimm_name;
    // Number of read and write operations performed from/to the physical media. 
    // Each operation transacts a 64byte operation. These operations includes 
    // commands transacted for maintenance as well as the commands transacted 
    // by the CPU.
    uint64_t    read_64B_ops_received;
    uint64_t    write_64B_ops_received;

    // Number of read and write operations received from the CPU (memory controller)
    // unit: 64 byte
    uint64_t    ddrt_read_ops;      
    uint64_t    ddrt_write_ops;

    // actual byte write/read to DIMM media
    // bytes_read (derived)   : number of bytes transacted by the read operations
    // bytes_written (derived): number of bytes transacted by the write operations
    // Note: The total number of bytes transacted in any sample is computed as 
    // bytes_read (derived) + 2 * bytes_written (derived).
    // 
    // Formula:
    // bytes_read   : (read_64B_ops_received - write_64B_ops_received) * 64
    // bytes_written: write_64B_ops_received * 64
    uint64_t    bytes_read;
    uint64_t    bytes_written;

    // Number of bytes received from the CPU (memory controller)
    // ddrt_read_bytes  : (ddrt_read_ops * 64)
    // ddrt_write_bytes : (ddrt_write_ops * 64)
    uint64_t    ddrt_read_bytes;      
    uint64_t    ddrt_write_bytes;

    // DIMM ; read_64B_ops_received  ; write_64B_ops_received ; ddrt_read_ops  ; ddrt_write_ops;
    // DIMM0; 1292533678412          ; 643726680616           ; 508664958085   ; 560639638344;
    void Parse(const std::string& result) {
        // printf("Parse: %s\n", result.c_str());
        std::vector<std::string> infos = split(result, ";");
        dimm_name = infos[0];
        read_64B_ops_received  = stol(infos[1]);
        write_64B_ops_received = stol(infos[2]);
        ddrt_read_ops          = stol(infos[3]);
        ddrt_write_ops         = stol(infos[4]);
        ddrt_read_bytes        = ddrt_read_ops * 64;
        ddrt_write_bytes       = ddrt_write_ops * 64; 
        bytes_read             = (read_64B_ops_received - write_64B_ops_received) * 64;
        bytes_written          = write_64B_ops_received * 64;
        // printf("%s", ToString().c_str());
    }

    std::string ToString() {
        std::string res;
        char buffer[1024];
        sprintf(buffer, "\033[34m%s | Read from IMC | Write from IMC | Read DIMM | Write DIMM |\n", dimm_name.c_str());
        res += buffer;
        sprintf(buffer, "  MB  | %13.0f | %14.0f | %9.0f | %10.0f |", 
            ddrt_read_bytes/1024.0/1024.0,
            ddrt_write_bytes/1024.0/1024.0,
            read_64B_ops_received*64/1024.0/1024.0,
            write_64B_ops_received*64/1024.0/1024.0);
        res += buffer;
        res += "\033[0m\n";
        return res;
    }

    std::vector<std::string> split(std::string str, std::string token){
        std::vector<std::string>result;
        while(str.size()){
            int index = str.find(token);
            if(index!=std::string::npos){
                result.push_back(str.substr(0,index));
                str = str.substr(index+token.size());
                if(str.size()==0)result.push_back(str);
            }else{
                result.push_back(str);
                str = "";
            }
        }
        return result;
    }
};

class IPMMetric {
public:
    IPMMetric(const IPMInfo& before, const IPMInfo& after) {
        metric_.bytes_read = after.bytes_read - before.bytes_read;
        metric_.bytes_written = after.bytes_written - before.bytes_written;
        metric_.ddrt_read_bytes = after.ddrt_read_bytes - before.ddrt_read_bytes;
        metric_.ddrt_write_bytes = after.ddrt_write_bytes - before.ddrt_write_bytes;
    }

    uint64_t GetByteReadToDIMM() {
        return metric_.bytes_read;
    }

    uint64_t GetByteWriteToDIMM() {
        return metric_.bytes_written;
    }

    uint64_t GetByteReadFromIMC() {
        return metric_.ddrt_read_bytes;
    }

    uint64_t GetByteWriteFromIMC() {
        return metric_.ddrt_write_bytes;
    }
private:
    IPMInfo metric_;
};

std::string zExecute(const std::string& cmd) {
    std::array<char, 128> buffer;
    std::string result;
    std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd.c_str(), "r"), pclose);
    if (!pipe) {
        throw std::runtime_error("popen() failed!");
    }
    while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
        result += buffer.data();
    }
    return result;
}

class IPMWatcher {
public:
    
    IPMWatcher(const std::string& name): file_name_("ipm_" + name + ".txt") {
        printf("\033[32mStart IPMWatcher.\n\033[0m");
        metrics_before_ = Profiler();
    }

    ~IPMWatcher() {
        metrics_after_ = Profiler();
        for (int i = 0; i < metrics_before_.size(); ++i) {
            auto& info_before = metrics_before_[i];
            auto& info_after  = metrics_after_[i];
            IPMMetric metric(info_before, info_after);
            std::string res;
            char buffer[1024];
            sprintf(buffer, "\033[34m%s | Read from IMC | Write from IMC | Read DIMM | Write DIMM |   RA   |   WA   |\n", info_before.dimm_name.c_str());
            res += buffer;
            sprintf(buffer, "  MB  | %13.4f | %14.4f | %9.4f | %10.4f | %6.2f | %6.2f |", // Read: %6.2f MB/s, Write: %6.2f MB/s", 
                    metric.GetByteReadFromIMC()/1024.0/1024.0,
                    metric.GetByteWriteFromIMC() /1024.0/1024.0,
                    metric.GetByteReadToDIMM() /1024.0/1024.0,
                    metric.GetByteWriteToDIMM() /1024.0/1024.0,
                    (double) metric.GetByteReadToDIMM() / metric.GetByteReadFromIMC(),
                    (double) metric.GetByteWriteToDIMM() / metric.GetByteWriteFromIMC()
                    // write_throughput
                    );
            res += buffer;
            res += "\033[0m\n";
            printf("%s", res.c_str());
        }   
        printf("\033[32mDestroy IPMWatcher.\n\033[0m\n");
        fflush(nullptr);
        zExecute("rm -f " + file_name_);
    }

    void Checkpoint() {
        metrics_after_ = Profiler();
        for (int i = 0; i < metrics_before_.size(); ++i) {
            auto& info_before = metrics_before_[i];
            auto& info_after  = metrics_after_[i];
            IPMMetric metric(info_before, info_after);
            std::string res;
            char buffer[1024];
            sprintf(buffer, "\033[34m%s | Read from IMC | Write from IMC | Read DIMM | Write DIMM |   RA   |   WA   |\n", info_before.dimm_name.c_str());
            res += buffer;
            sprintf(buffer, "  MB  | %13.4f | %14.4f | %9.4f | %10.4f | %6.2f | %6.2f |", // Read: %6.2f MB/s, Write: %6.2f MB/s", 
                    metric.GetByteReadFromIMC()/1024.0/1024.0,
                    metric.GetByteWriteFromIMC() /1024.0/1024.0,
                    metric.GetByteReadToDIMM() /1024.0/1024.0,
                    metric.GetByteWriteToDIMM() /1024.0/1024.0,
                    (double) metric.GetByteReadToDIMM() / metric.GetByteReadFromIMC(),
                    (double) metric.GetByteWriteToDIMM() / metric.GetByteWriteFromIMC()
                    // write_throughput
                    );
            res += buffer;
            res += "\033[0m\n";
            printf("%s", res.c_str());
        }   
        //printf("\033[32mDestroy IPMWatcher.\n\033[0m\n");
        fflush(nullptr);
        zExecute("rm -f " + file_name_);
    }

    double CheckWriteAmplification() {
        metrics_after_ = Profiler();
        if (metrics_before_.size() > 1) {
            printf("Error, more than one DIMM is recorded.\n");
            exit(1);
        } else {
            auto& info_before = metrics_before_[0];
            auto& info_after  = metrics_after_[0];
            IPMMetric metric(info_before, info_after);
            double wa = (double) metric.GetByteWriteToDIMM() / metric.GetByteWriteFromIMC();
            zExecute("rm -f " + file_name_);
            return wa;
        }
    }

    uint64_t CheckDataWriteToDIMM() {
        metrics_after_ = Profiler();
        if (metrics_before_.size() > 1) {
            printf("Error, more than one DIMM is recorded.\n");
            exit(1);
        } else {
            auto& info_before = metrics_before_[0];
            auto& info_after  = metrics_after_[0];
            IPMMetric metric(info_before, info_after);
            uint64_t dm = metric.GetByteWriteToDIMM();
            zExecute("rm -f " + file_name_);
            return dm;
        }
    }

    std::vector<IPMInfo> Profiler() const {
        std::vector<IPMInfo> infos;
        zExecute("/opt/intel/ipmwatch/bin64/ipmwatch -l >" + file_name_);
        std::string results = zExecute("grep -w \'DIMM0\' " + file_name_);
        std::stringstream ss(results);
        while (!ss.eof()) {
            std::string res;
            ss >> res;
            if (res.empty()) break;
            IPMInfo tmp;
            tmp.Parse(res);
            infos.push_back(tmp);
        }
        return infos;
    }

private:
    const std::string file_name_;
    std::vector<IPMInfo> metrics_before_;
    std::vector<IPMInfo> metrics_after_;
    uint64_t start_time_;
    uint64_t end_time_;
};

class WriteAmplificationWatcher {
public:
    WriteAmplificationWatcher(const IPMWatcher& watcher): watcher_(watcher) {
        auto tmp = watcher_.Profiler();
        before_ = watcher_.Profiler();
        for (int i = 0; i < before_.size(); ++i) {
            IPMMetric metric(tmp[i], before_[i]);
            dimm_bias_.push_back(metric.GetByteWriteToDIMM());
            imc_bias_.push_back(metric.GetByteWriteFromIMC());
        }
        
    }

    ~WriteAmplificationWatcher() {
        after_ = watcher_.Profiler();
        for (int i = 0; i < before_.size(); ++i) {
            IPMMetric metric(before_[i], after_[i]);
            printf("%s. WA(Bias): %2.4f. Write to DIMM: %10lu. Write from IMC: %10lu \n%s. WA(Fix) : %2.4f. Write to DIMM: %10lu. Write from IMC: %10lu \n", 
                before_[i].dimm_name.c_str(),
                (double)metric.GetByteWriteToDIMM() / metric.GetByteWriteFromIMC(),
                metric.GetByteWriteToDIMM(),
                metric.GetByteWriteFromIMC(),
                before_[i].dimm_name.c_str(),
                ((double)metric.GetByteWriteToDIMM() - dimm_bias_[i]) / (metric.GetByteWriteFromIMC() - imc_bias_[i]),
                metric.GetByteWriteToDIMM() - dimm_bias_[i],
                metric.GetByteWriteFromIMC() - imc_bias_[i]
                );
        }
    }

private:
    const IPMWatcher& watcher_;
    std::vector<IPMInfo> before_;
    std::vector<IPMInfo> after_;
    std::vector<uint64_t> dimm_bias_;
    std::vector<uint64_t> imc_bias_;
};

