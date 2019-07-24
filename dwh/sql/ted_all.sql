select
  t1.*,
  t2.transcript
from ted.ted_main t1
  join ted.transcripts t2 on t1.url = t2.url
where t2.url is not NULL;