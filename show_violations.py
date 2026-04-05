import json
v = json.loads(open('violation_log/violations.jsonl').readline())
print('BLAST RADIUS')
for s in v['blast_radius']['registry_subscribers']:
    print(f'  {s["subscriber_id"]:25}  mode={s["validation_mode"]}')
print()
print('BLAME CHAIN')
b = v['blame_chain'][0]
print(f'  author={b["author"]}  confidence={b["confidence_score"]}')
print(f'  {b["commit_message"]}')
