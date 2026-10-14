import subprocess,signal,os
from pathlib import Path
p=Path('/tmp/latticedb-mmap-20261014')
env=os.environ.copy();env.pop('LATTICE_BUFFER_POOL_MB',None)
proc=subprocess.Popen([str(p/'zig-snapshot'),'/tmp/latticedb-vector-opt-20261013/fixtures/vectors-100000.bin',str(p/'zig-100k.db')],stderr=subprocess.PIPE,text=True,env=env)
with (p/'zig-100k.txt').open('w') as log:
 count=0
 for line in proc.stderr:
  log.write(line);log.flush()
  if line.startswith('SNAPSHOT '):
   count+=1
   _, status = os.waitpid(proc.pid, os.WUNTRACED)
   assert os.WIFSTOPPED(status) and os.WSTOPSIG(status) == signal.SIGSTOP
   data=subprocess.check_output(['/usr/bin/vmmap','-summary',str(proc.pid)])
   (p/f'zig-{count}-{line.split()[1]}.vmmap').write_bytes(data)
   os.kill(proc.pid,signal.SIGCONT)
 code=proc.wait();log.write(f'exit={code}\n')
 if code: raise SystemExit(code)
