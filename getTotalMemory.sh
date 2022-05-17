totalmem=0;
for mem in /sys/devices/system/memory/memory*; do
  [[ "$(cat ${mem}/online)" == "1" ]] \
    && totalmem=$((totalmem+$((0x$(cat /sys/devices/system/memory/block_size_bytes)))));
done


#echo ${totalmem} bytes
echo $((totalmem/1024**3))GB
