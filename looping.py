from htcmap import htcmap
import sys


@htcmap
def double(x):
    return 2 * x


results = []
for x in range(10):
    results.append(double(x))

# print(results)

with double.build_job() as job_builder:
    print(job_builder)
    for x in range(10):
        job_builder(x)

print(job_builder.results)

# for r in job_builder.results:
#     print(r)
