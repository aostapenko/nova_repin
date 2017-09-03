.Phony: push

push :
	@echo $(PWD)
	rsync -e "ssh -i /home/shi/.ssh/harmonix-temp-key" -av $(PWD) root@172.17.1.131:~
