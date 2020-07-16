cat <<EOF | tee /etc/profile.d/10-<%= app_name %>.sh
#!/bin/sh
export PATH=$PATH:/opt/<%= app_name %>/bin/
EOF
chmod +x /etc/profile.d/10-<%= app_name %>.sh
