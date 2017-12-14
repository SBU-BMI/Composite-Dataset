docker exec -it quip-loader bash \
python3 /root/dataloader/DataLoader/dataLoader/dataloader.py \
-i /data/home/bwang/loader/input.csv \
-o http://quip-data:9099/services/Camicroscope_DataLoader/DataLoader/submit/json \
-a 4fbb38a3-1821-436c-a44d-8d3bc5efd33e
