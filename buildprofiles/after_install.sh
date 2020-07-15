cat <<EOF | tee /etc/profile.d/10-<%= name %>.sh
#!/bin/sh
export PATH=$PATH:/opt/<%= name %>/bin/
EOF
