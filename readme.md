version 2.0.13\
docker compose build --no-cache\
docker compose up -d

for export image\
docker save --output mic_mms_app_213.tar mic/mms_app:2.0.13

for import image\
docker load -i mic_mms_app_213.tar