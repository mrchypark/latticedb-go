import subprocess,signal,os
from pathlib import Path
p=Path('/tmp/latticedb-mmap-20261014')
env=os.environ.copy();env.pop('GOGC',None);env.pop('GOMEMLIMIT',None);env['GOMAXPROCS']='2'
proc=subprocess.Popen([str(p/'external.test'),'-test.run=^$','-test.bench=^BenchmarkVectorSearchDisk128D$/^100K$','-test.benchtime=1x','-test.timeout=9m'],stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,env=env)
with (p/'external-100k.txt').open('w') as log:
 for line in proc.stdout:
  log.write(line);log.flush()
  if line.startswith('SNAPSHOT '):
   _,status=os.waitpid(proc.pid,os.WUNTRACED)
   assert os.WIFSTOPPED(status) and os.WSTOPSIG(status)==signal.SIGSTOP
   data=subprocess.check_output(['/usr/bin/vmmap','-summary',str(proc.pid)])
   (p/('external-'+line.split()[1]+'.vmmap')).write_bytes(data)
   os.kill(proc.pid,signal.SIGCONT)
 _,status,usage=os.wait4(proc.pid,0)
 code=os.waitstatus_to_exitcode(status);proc.returncode=code
 log.write(f'exit={code} user={usage.ru_utime} sys={usage.ru_stime} maxrss_bytes={usage.ru_maxrss}\n')
 if code:raise SystemExit(code)
