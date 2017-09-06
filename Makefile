.Phony: push

push :
	@echo $(PWD)
	rsync -e "ssh -i /home/shi/.ssh/harmonix-temp-key" -av $(PWD) root@pinning-repin:~
