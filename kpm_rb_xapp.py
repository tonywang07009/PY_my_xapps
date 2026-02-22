import sys
import os
import time
import pdb
import random  

''' Call swig compile C libary function '''    
sys.path.append("/flexric_src/build-multi/examples/xApp/python3") # The docker xapps module role                                                 # Need the compile finshed  
import xapp_sdk as ric


# Control DEBUG print level
DEBUG = ("--DEBUG" in sys.argv) # The Debug mode switch 
# run: python3 kpm_rb_xapp.py --DEBUG

#  This's handle the data transformer 
#  Now need measure the RB usage and do the control
# ===== MACCallback
class MACCallback(ric.mac_cb): 
    def __init__(self,owner):
        super().__init__() # Call the parent class constructor
        self.owner = owner
        self.prev_prb = {} # key   : rnti 
                           # value : last ul_aggr_prb 
                           # job : store the last prb states

    def handle(self,ind):
        # TODO: after ind_msg will parse the {ue,rb_usage}
        if DEBUG:
            print("[DEBUG] ue_0 fields:",dir(ind.ue_stats))
            print("[MACCallBack] indication received ")

        if len(ind.ue_stats) == 0:
            print("[MACCallback] no ue_stats in indication")
            return
        
        # The ue kpm catch 
        for ue_stat in ind.ue_stats:
            ue = ue_stat.rnti
        #== The prb recoding area ==
            curr_prb = float(ue_stat.ul_aggr_prb) # The current prb usage
            prev_prb = self.prev_prb.get(ue,curr_prb) # The key protect
            delta_prb = max(curr_prb - prev_prb,0.0) # The prb increase
            self.prev_prb[ue] = curr_prb # The prb update

            kpm_msg ={
            "ue":ue,
            "delta_prb": delta_prb
        }

            self.owner.on_kpm_report(kpm_msg)
        

        # if DEBUG:
        #     attrs = [a for a in dir(ue_0)if not a.startswith('_')]
        #     print("[MACCallback] ue_0 attrs : ",attrs)

   
        #=== The prb usage ====
        max_prb_window = 100.0 # The prb window size
        # rb_usage = min (delta_prb / max_prb_window,1.0) # The rb usage calculate

        print(f"[MAC] ue={ue}, curr_ul_aggr_prb={curr_prb:.1f}, "
              f"delta_ul_prb={delta_prb:.1f}")

        # print(f"[MAC] ue={ue}," 
        #       f"ul_sched_rb={delta_prb:.1f},"  # The display different rb usage 
        #       f"rb_usage={rb_usage:.3f}\n,")
        

        # the self owner space is store the kpm_msg data sturct 


class Kpm_Rb_Xapp:
    def __init__(self):

        # step 1 build the sdk connection
        print("[init] start")
        ric.init() # The connect with ric
        print("[init] ric.init() ok")
        
        # step 2 get the E2 node list
        self.conn:list = ric.conn_e2_nodes()
        self.connected = len(self.conn) > 0  # chechk have any gnb open
        print(f"[init] conn_e2_nodes len={len(self.conn)}")

        # step 3: set connection status; 
        self.kpm_buffer:list=[]
        self.rb_config:dict ={}
        self.mac_callback_list = []
        self.ul_threshold = 0.7
        # step 4 : The sample accroding parameter
        # sampling parameter. 
        self.prb_per_slot = 106
        self.batch_size = 10 
        self.sample_size = 3 
        self.batch_buffer = [] 

        for member in self.conn:
            mac_cb =  MACCallback(self)
            handle = ric.report_mac_sm(member.id , ric.Interval_ms_10,mac_cb)
            self.mac_callback_list.append((mac_cb,handle))
        
# ===== aggregate batch 抽樣計畫

    def aggregate_batch(self):

        # step 1. from batch_buffer sampling sample_size 
        if(len(self.batch_buffer)<= self.sample_size):
            samples = self.batch_buffer[-self.sample_size:] # All get if insufficient.
        else:
            samples = random.sample(self.batch_buffer,self.sample_size)     
                                  # The maternal , sampling size

        # step 2. calculate the average rb_usage -> the sample reduce
        rb_list = []

        for sample in samples:
            rb_list.append(sample["kpm_message"]["delta_prb"])
        
        sum_delta = sum(rb_list)

        # The PRB slot clculate
        max_capacity = self.prb_per_slot * len(samples) # The max prb capacity in the sample window
        rb_usage = 0.0

        if max_capacity > 0:
            rb_usage = min(sum_delta / max_capacity,1.0)

        # step 3. The sample first ue is symbol ue 
        ue = samples[0]["kpm_message"]["ue"]

        avg_rb = sum_delta / max_capacity if max_capacity > 0 else 0.0
        # step 4. make this sample symbol recoding  
        # and put in the kpm_buffer for long store
        record = {
            
                "time_stamp":time.time(),
                "kpm_message":{
                
                    "ue":ue,
                    "rb_usage":rb_usage
                }
            }
         
        self.kpm_buffer.append(record)

        if(len(self.kpm_buffer) > 1000):
            self.kpm_buffer.pop(0)

        time_stamp =record["time_stamp"]
        t_s_str = time.strftime("%H:%M:%S", time.localtime(time_stamp))
        
        print(f" [ aggregate {t_s_str} ] batch_size = {len(self.batch_buffer)},"
              f" avg_rb={rb_usage:.2f}, sum_delta={sum_delta:.1f},"
              f" min={min(rb_list):.2f}, max={max(rb_list):.2f}")

        # step 5. Use this sample info do decision make Rb Policy
        self.apply_rb_control(record["kpm_message"])

# ===== on_kpm_report
    def on_kpm_report(self,kpm_msg):
        
        # step 1 The kpm_msg display
        if DEBUG:
            print(f"[on_kpm_report] recv kpm_msg={kpm_msg}\n")
        
        # sample put in buffer sample
        self.batch_buffer.append({
        
            "time_stamp": time.time(),
            "kpm_message": kpm_msg

        })  

         # step 2 The sample size condiction 
        if (len(self.batch_buffer) < self.batch_size):
            return
        
        #  step 3 Build sample sturct
        #  make sample + average rb_usage -> kpm_buffer
        self.aggregate_batch()

        #  step 4 Buffer content clear
        self.batch_buffer.clear()
        

# ===== raw_buffer
    def raw_buffer(self,kpm_msg):
        # step 1. join the kpm with time stamp sturct
        record = {

            "time_stamp":time.time(),
            "kpm_message":kpm_msg
        }

        # step 2. join the buffer area
        self.kpm_buffer.append(record)

        # step 3. print validation  this way only print profile , didn't really control  
        if DEBUG:
            print(f"[raw_buffer] size = {len(self.kpm_buffer)}")

# ===== rb_control 
# ===== sample trans profile
    def apply_rb_control(self,kpm_msg):

        # step 1. bondry check
        if (not self.kpm_buffer):
            if DEBUG:
                print(f"Don't have any kpm_buffer into{self.kpm_buffer}")
            return

        # step 2. get the data content 
        ue_last = kpm_msg["ue"]
        ul_usage = kpm_msg["rb_usage"]

        # step 3. The ue behavior check
        #  logic is decision make 
        if (ul_usage > self.ul_threshold):
            self.rb_config[ue_last] = "limit" # The into the sign
        else:
            self.rb_config[ue_last] =  "normal"

        # step 4 . Display control content
        print(f"[rb_ctrl] ue      = {ue_last},"
                          f" ul_usage= {ul_usage:.2f},"
                          f" profile = {self.rb_config[ue_last]}\n")           

#===== run
    def run(self):
        # step 1. bondry check
        if not self.connected :
            print("No E2 Nodes connected, xApp will not start control loop. ")
            return  # TODO: __main__ control loop

        # step 2. used the xapp threads     
        print("Starting Main loop, wating for MAC indications...")
        ric.xapp_wait()

        # step 3 kepp stay life
        while True:
            time.sleep(1) # 主執行緒保持活著即可


#===== stop
    def stop(self):
        print("bye")


#=====
#=====main function


# I/o  system       
if __name__ == "__main__":
    app = Kpm_Rb_Xapp()

    try:
        app.run()

    except KeyboardInterrupt:
        print("interrupted by user  ")

 
    finally:
        app.stop()
        print(ric)