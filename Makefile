start-session:
	tmux attach-session -t crowler

delete-session:
	tmux kill-session -t crowler

create-session:
	tmux new-session -s crowler
