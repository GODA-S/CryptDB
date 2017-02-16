#!/usr/bin/env python
# makeavg_timelog.py
import sys
import re
args = sys.argv

print 'ExeName:' + args[0]
print 'fileName:' + args[1]
print 'start...'

#f:source file (raw log data)
f = open(args[1],'r')
#rf:result file(output summary)
rf = open(args[2],'w')

rf.write("makeavg_timelog\n")
rf.write("filename:" + args[1] + "\n")
rf.write("--------------------\n")

time = [[0 for i in range(8)]for j in range(35)]
timecount = [0 for i in range(35)]
summary_querytype = [[1,5,6,10,12,13,14,15,16,20,21,22,23,24,25,27,32],[33,34],[0],[30],[3,4,8,19],[26],[7,17,18,28,29],[2,9,11,31]]
summary_queryname = ["select_equal","select_range","select_join","select_sum","insert","delete","update_set","update_inc"]
summary_querycount = [0 for i in range(8)]
summary_querytime = [[0 for i in range(8)]for j in range(8)]

querynum = 0
patternlist_real = [r"SELECT c_discount, c_last, c_credit, w_tax FROM customer, warehouse WHERE w_id = .*? AND c_w_id = w_id AND c_d_id = .*? AND c_id = .*?",
r"SELECT d_next_o_id, d_tax FROM district WHERE d_id = .*? AND d_w_id = .*?",
r"UPDATE district SET d_next_o_id = .*? WHERE d_id = .*? AND d_w_id = .*?",
r"INSERT INTO orders \(o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local\) VALUES\(.*?, .*?, .*?, .*?, .*?, .*?, .*?\)",
r"INSERT INTO new_orders \(no_o_id, no_d_id, no_w_id\) VALUES \(.*?,.*?,.*?\)",
r"SELECT i_price, i_name, i_data FROM item WHERE i_id = .*?",
r"SELECT s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 FROM stock WHERE s_i_id = .*? AND s_w_id = .*?",
r"UPDATE stock SET s_quantity = .*? WHERE s_i_id = .*? AND s_w_id = .*?",
r"INSERT INTO order_line \(ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info\) VALUES \(.*?,.*?,.*?,.*?,.*?,.*?,.*?,.*?,.*?\)",
r"UPDATE warehouse SET w_ytd = .*? WHERE w_id = .*?",
r"SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name FROM warehouse WHERE w_id = .*?",
r"UPDATE district SET d_ytd = .*? WHERE d_w_id = .*? AND d_id = .*?",
r"SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name FROM district WHERE d_w_id = .*? AND d_id = .*?",
r"SELECT count\(c_id\) FROM customer WHERE c_w_id = .*? AND c_d_id = .*? AND c_last = .*?",
r"SELECT c_id FROM customer WHERE c_w_id = .*? AND c_d_id = .*? AND c_last = .*? ORDER BY c_first",
r"SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since FROM customer WHERE c_w_id = .*? AND c_d_id = .*? AND c_id = .*?",
r"SELECT c_data FROM customer WHERE c_w_id = .*? AND c_d_id = .*? AND c_id = .*?",
r"UPDATE customer SET c_balance = .*?, c_data = .*? WHERE c_w_id = .*? AND c_d_id = .*? AND c_id = .*?",
r"UPDATE customer SET c_balance = .*? WHERE c_w_id = .*? AND c_d_id = .*? AND c_id = .*?",
r"INSERT INTO history\(h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data\) VALUES\(.*?, .*?, .*?, .*?, .*?, .*?, .*?, .*?\)",
r"SELECT count\(c_id\) FROM customer WHERE c_w_id = .*? AND c_d_id = .*? AND c_last = .*?",
r"SELECT c_balance, c_first, c_middle, c_last FROM customer WHERE c_w_id = .*? AND c_d_id = .*? AND c_last = .*? ORDER BY c_first",
r"SELECT c_balance, c_first, c_middle, c_last FROM customer WHERE c_w_id = .*? AND c_d_id = .*? AND c_id = .*?",
r"SELECT o_id, o_entry_d, o_carrier_id FROM orders WHERE o_w_id = .*? AND o_d_id = .*? AND o_c_id = .*? AND o_id = .*?",
r"SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM order_line WHERE ol_w_id = .*? AND ol_d_id = .*? AND ol_o_id = .*?",
r"SELECT MIN\(no_o_id\) FROM new_orders WHERE no_d_id = .*? AND no_w_id = .*?",
r"DELETE FROM new_orders WHERE no_o_id = .*? AND no_d_id = .*? AND no_w_id = .*?",
r"SELECT o_c_id FROM orders WHERE o_id = .*? AND o_d_id = .*? AND o_w_id = .*?",
r"UPDATE orders SET o_carrier_id = .*? WHERE o_id = .*? AND o_d_id = .*? AND o_w_id = .*?",
r"UPDATE order_line SET ol_delivery_d = .*? WHERE ol_o_id = .*? AND ol_d_id = .*? AND ol_w_id = .*?",
r"SELECT SUM\(ol_amount\) FROM order_line WHERE ol_o_id = .*? AND ol_d_id = .*? AND ol_w_id = .*?",
r"UPDATE customer SET c_balance = c_balance \+ .*? , c_delivery_cnt = c_delivery_cnt \+ 1 WHERE c_id = .*? AND c_d_id = .*? AND c_w_id = .*?",
r"SELECT d_next_o_id FROM district WHERE d_id = .*? AND d_w_id = .*?",
r"SELECT DISTINCT ol_i_id FROM order_line WHERE ol_w_id = .*? AND ol_d_id = .*? AND ol_o_id < .*? AND ol_o_id >= .*?",
r"SELECT count\(\*\) FROM stock WHERE s_w_id = .*? AND s_i_id = .*? AND s_quantity < .*?"]
patternlist = [0]*35
for i in range(0,35):
  patternlist[i] = re.compile(patternlist_real[i])

prequery_real = r"\(log_text\): QUERY:"
prequery = re.compile(prequery_real)

pretime_real = r"\(timelog_output\):"
pretime = re.compile(pretime_real)

timelist_real = [r"before_allprocess:",r"rewrite:",r"before_execute_query:",r"execute_query:",r"before_disp_encresult:",r"disp_encresult:",r"disp_decresult:",r"SUM:"]
timelist = [0]* 8
for i in range(0,8):
  timelist[i] = re.compile(timelist_real[i])

for line in f:
  if re.search(prequery,line):
    for i in range(0,35):
      if re.search(patternlist[i],line):
        timecount[i] += 1
        querynum = i
        for n in range(0,8):
          if querynum in summary_querytype[n]:
            summary_querycount[n] += 1
  if re.search(pretime,line):
    for i in range(0,8):
      if re.search(timelist[i],line):
	time[querynum][i] += float(line[re.search(timelist[i],line).end():])
        for n in range(0,8):
          if querynum in summary_querytype[n]:
            summary_querytime[n][i] += float(line[re.search(timelist[i],line).end():])

for i in range(0,35):
  rf.write("\n\n----------------------------\n")
  rf.write(str(i) + ":" + patternlist_real[i] + "\n")
  rf.write("*****SUM*****\n")
  rf.write("count" + str(timecount[i]) + "\n")
  for j in range(0,8):
    rf.write(timelist_real[j] + str(time[i][j]) + "\n")
  rf.write("\n")
  rf.write("*****AVERAGE*****\n")
  for j in range(0,8):
    if timecount[i] !=0:
      rf.write(timelist_real[j] + str(time[i][j]/timecount[i]) + "\n")
    else:
      rf.write(timelist_real[j] + "0\n")
      
for i in range(0,8):
  rf.write("\n\n----------------------------\n")
  rf.write("\n\n----------------------------\n")
  rf.write(summary_queryname[i] + ":\n")
  rf.write("*****SUM*****\n")
  rf.write("count" + str(summary_querycount[i]) + "\n")
  for j in range(0,8):
    rf.write(timelist_real[j] + str(summary_querytime[i][j]) + "\n")
  rf.write("\n")
  rf.write("*****AVERAGE*****\n")
  for j in range(0,8):
    if timecount[i] !=0:
      rf.write(timelist_real[j] + str(summary_querytime[i][j]/summary_querycount[i]) + "\n")
    else:
      rf.write(timelist_real[j] + "0\n")



print "end"
f.close()
