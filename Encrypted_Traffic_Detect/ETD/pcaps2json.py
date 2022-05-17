import os

def pcap2json():
	command = '../joy/bin/joy tls=1 bidir=1 dist=1 num_pkts=50 zeros=0 retrans=0 entropy=1 ./pcaps/*.pcap | gunzip | ../joy/sleuth --where "tls=*"'
	print ("joy is parsing pcaps files...")
	out = os.popen(command)
	print ("finish")
	json_stat = out.read()
	lines = json_stat.split('\n')
	return lines
