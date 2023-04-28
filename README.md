# Database-Transaction-Scheduler

## Introduction:

Implementing a programme that simulates the execution of a given schedule while adhering to the concurrency control principles of recoverability and cascadeless recoverability was necessary for the work at hand. The difficulties encountered throughout the programme's development are highlighted in this report.

Understanding concurrency control's guiding concepts presented the initial difficulty. Once a transaction is committed, the database must continue to save its modifications in order for recovery to be possible. Contrarily, cascadeless recoverability demands that no transaction's execution be able to result in a series of rollbacks. Extensive study of the two principles and how they are applied in database management systems was done in order to overcome this problem.

The next problem was creating an architecture that followed concurrency control rules after comprehending these principles. It was necessary for the program to be able to read data from an input file, simulate the execution of the procedures, and show the actual sequence in which the actions were carried out. In case of future changes, the architecture has to be adaptable and simple to maintain. A “scheduler” interface was developed, and implemented for each type of scheduler: cascading recoverable, recoverable, serializable, serial and 

The program’s testing was the last difficulty. It was essential to make sure the program carried out the activities in the right order while abiding by concurrency control rules because the programme dealt with these principles. To assure the program’s accuracy, extensive testing was done on it, including stress testing, boundary testing, and testing on real-life schedules. The problem was solved by developing a thorough test plan and carefully carrying it out.


Conclusion It's a difficult effort that calls for substantial testing and knowledge to implement a programme that follows concurrency control concepts. Understanding concurrency control principles, creating the programme architecture, and testing the programme were obstacles encountered throughout the creation of this program. But these difficulties can be solved with the correct strategy and commitment, leading to a program that executes activities in accordance with concurrency control principles.





## Running the Program

In order to run this program, extract all the program files (if using the zip file). Ensure that this program is run on a Linux machine with a g++ compiler >=  11. 

Run the command:

```
make run INPUT_FILE=’input_file_path.txt' 
```




## Implementing a Recoverable Scheduler

A recoverable schedule is a type of schedule that guarantees recoverability in the event of a transaction failure. In other words, if a transaction fails and aborts midway through, the schedule must be recoverable, meaning that it can restore the system to a state that is consistent with the transactions that have already committed. Implementing a recoverable schedule is critical in database systems because it ensures data consistency and minimizes data loss.

To implement a recoverable schedule, one must follow certain rules. Firstly, no transaction should overwrite uncommitted data in the database, as this could result in data loss if the transaction fails. Secondly, if a transaction T1 modifies data that is then read by another transaction T2, T2 must not commit until T1 has either committed or aborted. This ensures that if T1 fails, T2 can still read the correct data. Finally, a transaction must not commit until all transactions that have modified data that T1 has read or modified have committed.

To achieve these rules, I utilized the concept of transaction logs, which record all of the actions performed by a transaction. Each transaction has a unique identifier and is recorded in the log, along with the type of action (read, write, commit, or abort) and the data item that was accessed. If a transaction fails, the log can be used to undo any changes made by the transaction.

To implement a recoverable schedule, I used a wait-die scheme. In this scheme, a transaction can wait for a conflicting transaction to finish if the waiting transaction has a higher priority than the conflicting transaction. If the conflicting transaction has a higher priority, the waiting transaction is aborted and restarted later. This ensures that a lower priority transaction never overwrites uncommitted data from a higher priority transaction.


## Implementing a Cascadeless Recoverable Scheduler

A cascadeless recoverable schedule is a schedule that ensures no cascading rollbacks occur, meaning if a transaction is rolled back, it does not force other transactions to roll back as well. This is accomplished by using a deferred update strategy, which ensures that a transaction's updates are not made permanent until it is certain that the transaction will commit. This strategy prevents cascading rollbacks by allowing a transaction to be rolled back without affecting other transactions that may have read its uncommitted changes.

To implement a cascadeless recoverable scheduler, a deferred update strategy must be used along with a commit-time validation approach, which checks for conflicts between transactions at commit time. Transactions that pass the validation are allowed to commit, while those that do not are rolled back. To ensure correctness, a dependency graph can be constructed to identify potential conflicts between transactions, and the transactions can then be scheduled to reduce conflicts and maximize concurrency. While cascadeless recoverable schedules may not perform as well as serializable schedules in terms of speed, they are necessary in systems where data consistency and recoverability are critical.


## Implementing a Serial Scheduler

A schedule known as a serial schedule is one in which transactions are carried out consecutively, without any interleaving. In other words, before the next transaction starts, the previous one is fully completed. As a result, there are no concurrency control problems and the transactions are executed in total isolation from one another. Serial schedules guarantee correctness, but because they do not take use of concurrency or parallelism, they are not very effective in terms of performance.

I implemented this serial scheduler by storing all the actions that a particular action executes in order. I then sort all the transactions in the increasing order of the last action of a transaction (either a commit or abort). Then, all the actions of all the transactions in such an order are sequentially processed. It is important to note that this is an efficient way to schedule transactions sequentially, as the transactions are sorted on the last action time. In many scenarios, all the transactions can be processed in less time than the serializable schedule, and occasionally the cascading recoverable schedule because of this, however the average wait time for an action to be processed is significantly higher for many inputs. When an action comes in, although in aggregate the time to process all the transactions may be lower, the time that an action must wait is sometimes large, causing long and unexpected delays from a user’s perspective.


## Implementing a Serializable Scheduler

A schedule that executes transactions concurrently but makes them seem as though they were carried out in sequence is known as a serializable schedule. The usage of serializable schedules maintains correctness while enhancing performance and using concurrency and parallelism. Serializable schedules, as opposed to serial schedules, are capable of handling numerous concurrently running transactions while avoiding potential concurrency control problems.
I employed a two-phase locking approach to guarantee serializability in order to create a serializable scheduler. The transactions gain locks on the resources they have access to during the first phase, known as the growth phase. The transactions release their locks during the second phase, often known as the shrinking phase. This makes sure that no transaction can access a locked object by another transaction, either for reading or writing.


The transactions are scheduled in a way that prevents conflicts between transactions that access the same item in order to ensure serializability. I utilised a schedule that can be converted into a serial schedule while maintaining the order of all operations, known as a conflict-serializable schedule.

Because they support concurrency and parallelism, serializable schedules can be more effective than serial schedules in terms of speed. The performance, however, can be impacted by the order in which the transactions are carried out. Transactions may experience delays and system performance issues if they often access the same things and must wait for locks to be released. Because of this, it's critical to give careful thought to the scheduling order and make sure that transactions are scheduled in a way that reduces conflicts and maximises concurrency.



Example Input:

```
1 T1 W A
2 T2 W B
6 T3 R A
7 T3 R B
8 T3 C
9 T2 C
10 T1 R A
11 T1 C
```

Example Output:

```
./bin/main data/TC10.txt TRUE
Serial Scheduler
6 6 T3 R A
7 7 T3 R B
8 8 T3 C
9 2 T2 W B
10 9 T2 C
11 1 T1 W A
12 10 T1 R A
13 11 T1 C


Serializable Scheduler
1 1 T1 W A
2 2 T2 W B
9 9 T2 C
10 10 T1 R A
11 11 T1 C
12 6 T3 R A
13 7 T3 R B
14 8 T3 C


Cascadeless Recoverable
1 1 T1 W A
2 2 T2 W B
9 9 T2 C
10 10 T1 R A
11 11 T1 C
12 6 T3 R A
13 7 T3 R B
14 8 T3 C


Recoverable
1 1 T1 W A
2 2 T2 W B
6 6 T3 R A
7 7 T3 R B
9 9 T2 C
10 10 T1 R A
11 11 T1 C
12 8 T3 C

```
