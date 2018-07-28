#!/usr/bin/env python3

import pytest
import coverage
import os

cov = coverage.coverage()
cov.start()

pytest.main()

cov.stop()
cov.save()

print('Coverage Report:')
cov.report()

report_dir = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'covreport')
cov.html_report(directory = report_dir)
print(f'HTML Report at {os.path.join(report_dir, "index.html")}')

cov.erase()
