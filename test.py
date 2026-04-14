import MySQLdb
import sshtunnel

sshtunnel.SSH_TIMEOUT = 5.0
sshtunnel.TUNNEL_TIMEOUT = 5.0

# PythonAnywhere SSH connection details
ssh_hostname = 'ssh.pythonanywhere.com'
ssh_username = 'omolobe'
ssh_password = '#Just4pythonanywhere'

# MySQL database connection details
db_username = 'omolobe'
db_password = '#Just4swiftresolve'
db_hostname = 'omolobe.mysql.pythonanywhere-services.com'
db_name = 'omolobe$creditrustdb'

with sshtunnel.SSHTunnelForwarder(
        ssh_hostname,
        ssh_username=ssh_username, ssh_password=ssh_password,
        remote_bind_address=(db_hostname, 3306)
) as tunnel:
    connection = MySQLdb.connect(
        user=db_username,
        passwd=db_password,
        host='127.0.0.1', port=tunnel.local_bind_port,
        db=db_name,
    )

    connection.close()
