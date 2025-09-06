import subprocess
import sys

if __name__ == '__main__':
	cmd = ['streamlit', 'run', 'dashboard.py']
	print('Running:', ' '.join(cmd))
	sys.exit(subprocess.call(cmd)) 