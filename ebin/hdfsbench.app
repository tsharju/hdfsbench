{application,
      hdfsbench,
        [{description, "Thrift interface for HDFS"},
         {vsn, "0.1"},
         {modules, [
                     thriftHadoopFileSystem_thrift,
                     hadoopfs_types
                   ]},
         {registered, []},
         {applications, [kernel,stdlib,sasl]},
         {env, []}
        ]}.
