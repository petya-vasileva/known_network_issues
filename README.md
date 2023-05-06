This repository will keep track of discovered and later fixed network issues. 

The StoreRawData class loads all available data for the specified period. Then we can filter on any site or check the issues dictionary ("sitesInvolved") for the one(s) close to the source of the problem.

    {
        'sitesInvolved': 'BNL-ATLAS',
        'issueStart': '2023-01-30 18:15', # -6hours of the start time
        'issueEnd': '2023-02-01 14:15',  # +6hours of the end time
        'indices': ['ps_throughput', 'ps_trace'],
        'location': '/data/petya/parquet/raw/bnl_issue/'
    }
Added an offset of 6 hours just before the issue started and just after it was fixed.
