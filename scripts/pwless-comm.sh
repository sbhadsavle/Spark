ssh-keygen -f ~/.ssh/id_rsa -t rsa -P "" &&
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys &&
cat ~/.ssh/id_rsa.pub | ssh hdatanode1 'cat >> ~/.ssh/authorized_keys' &&
cat ~/.ssh/id_rsa.pub | ssh hdatanode2 'cat >> ~/.ssh/authorized_keys' &&
cat ~/.ssh/id_rsa.pub | ssh hdatanode3 'cat >> ~/.ssh/authorized_keys'
