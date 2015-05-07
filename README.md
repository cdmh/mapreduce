MapReduce C++ Library
=
The MapReduce C++ Library implements a single-machine platform for programming using the the Google MapReduce idiom. Users specify a map function that processes a key/value pair to generate a set of intermediate key/value pairs, and a reduce function that merges all intermediate values associated with the same intermediate key. Many real world tasks are expressible in this model, as shown in the Google paper.

    map (k1,v1) --> list(k2,v2)
    reduce (k2,list(v2)) --> list(v2)

Synopsis
-

```cpp
namespace mapreduce {

template<typename MapTask,
		 typename ReduceTask,
		 typename Datasource=datasource::directory_iterator<MapTask>,
		 typename Combiner=null_combiner,
		 typename IntermediateStore=intermediates::local_disk<MapTask> >
class job;

} // namespace mapreduce
```
    
The developer is required to write two classes; `MapTask` implements a mapping function to process key/value pairs generate a set of intermediate key/value pairs and `ReduceTask` that implements a reduce function to merges all intermediate values associated with the same intermediate key.
In addition, there are three optional template parameters that can be used to modify the default implementation behavior; `Datasource` that implements a mechanism to feed data to the Map Tasks - on request of the `MapReduce` library, Combiner that can be used to partially consolidate results of the Map Task before they are passed to the Reduce Tasks, and `IntermediateStore` that handles storage, merging and sorting of intermediate results between the Map and Reduce phases.
The `MapTask` class must define four data types; the key/value types for the inputs to the Map Tasks and the intermediate types.

```cpp
class map_task
{
  public:
	typedef std::string   key_type;
	typedef std::ifstream value_type;
	typedef std::string   intermediate_key_type;
	typedef unsigned      intermediate_value_type;

	map_task(job::map_task_runner &runner);
	void operator()(key_type const &key, value_type const &value);
};
```
      
The `ReduceTask` must define the key/value types for the results of the Reduce phase.

```cpp
class reduce_task
{
  public:
	typedef std::string  key_type;
	typedef size_t       value_type;

	reduce_task(job::reduce_task_runner &runner);

	template<typename It>
	void operator()(typename map_task::intermediate_key_type const &key, It it, It ite)
};
```

Extensibility
-
The library is designed to be extensible and configurable through a Policy-based mechanism. Default implementations are provided to enable the library user to run MapReduce simply by implementing the core Map and Reduce tasks, but can be replaced to provide specific features.

| Policy | Application | Supplied Implementation(s) |
| ------ | ---- | --- |
| `Datasource` | `mapreduce::job` template parameter | `datasource::directory_iterator<MapTask>` |
| `Combiner` | `mapreduce::job` template parameter | `null_combiner` |
| `IntermediateStore` | `mapreduce::job` template parameter | `local_disk<MapTask, SortFn, MergeFn>` |
| `SortFn` | `local_disk` template parameter | `external_file_sort` |
| `MergeFn` | `local_disk` template parameter | `external_file_merge` |
| `SchedulePolicy` | `mapreduce::job::run()` template parameter | `cpu_parallel`, `sequential` |

Datasource
-
This policy implements a data provider for Map Tasks. The default implementation iterates a given directory and feeds each Map Task with a `Filename` and `std::ifstream` to the open file as a key/value pair.
Combiner
-
A *Combiner* is an optimization technique, originally designed to reduce network traffic by applying a local reduction of intermediate key/value pairs in the Map phase before being passed to the Reduce phase. The combiner is optional, and can actually degrade performance on a single machine implementation due to the additional file sorting that is required. The default is therefore a null_combiner which does nothing.
IntermediateStore
-
The policy class implements the behavior for storing, sorting and merging intermediate results between the Map and Reduce phases. The default implementation uses temporary files on the local file system.
SortFn
-
Used to sort external intermediate files. Current default implementation uses a `system()` call to shell out to the operating system SORT process. A Merge Sort implementation is currently in development.
MergeFn
-
Used to merge external intermediate files. Current default implementation uses a system() call to shell out to the operating system `COPY` process (Win32 only). A platform independent in-process implementation is required.
SchedulePolicy
-
This policy is the core of the scheduling algorithm and runs the Map and Reduce Tasks. Two schedule policies are supplied, `cpu_parallel` uses the maximum available CPU cores to run as many map simultaneous tasks as possible (within a limit given in the `mapreduce::specification` object). The sequential scheduler will run one map task followed by one reduce task, which is useful for debugging purposes.

See the [MapReduce C++ Library](http://cdmh.co.uk/papers/software_scalability_mapreduce/library.php) page for more information, and a sample program.
