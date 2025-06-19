# MinIO tools

## Upload files, load test, switching between http and https

## Upload files

-= under development =-

## Load test

-= under development =-

## TLS - enable/disable

To enable TLS in MinIO run playbook:

```
git clone repo-url
cd minio/ansible
ansible-playbook -Ki inv.yaml -e "tls=on" minio_tls.yaml
```


To disable TLS in MinIO run playbook:

```
git clone repo-url
cd minio/ansible
ansible-playbook -Ki inv.yaml -e "tls=off" minio_tls.yaml
```

To enable or disable TLS on nginx you need to update `proxy_pass` directive from/to http/https and restart nginx. Playbook doesn't do it yet.
