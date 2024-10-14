package main

import (
	"container/list"
	"context"
	_ "flag"
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sync"

	//"reflect"
	"google.golang.org/grpc"
	_ "k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	v1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	pb "TaskStatusTracker/TaskStatusTrackerProto"
	tb "TaskStatusTracker/schedulerProto"
)

//const (
//	port = "192.168.1.109:50051"
//)
type server struct {
	pb.UnimplementedTaskStatusTrackerServer
}

//TaskListChan := make(chan *list.List, 10000)
var TaskListChan chan *list.List

var port string
var clientset *kubernetes.Clientset

var deletedPodNum int64 = 0

var trackerSema = make(chan int, 1)
var clusterId string

func grpcServer(waiter *sync.WaitGroup) {
	defer waiter.Done()
	lis, err := net.Listen("tcp", ":6060")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	log.Println("starting grpc server here.")
	pb.RegisterTaskStatusTrackerServer(s, &server{})
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
	log.Println("wait here")
}
func main() {

	//初始化配置文件
	//var kubeconfig *string
	//if home := homeDir(); home != "" {
	//	kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	//} else {
	//	kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	//}
	//flag.Parse()
	k8sconfig, err := filepath.Abs(filepath.Dir("/etc/kubernetes/kubelet.kubeconfig"))
	if err != nil {
		panic(err.Error())
	}
	// uses the current context in kubeconfig
	config, err := clientcmd.BuildConfigFromFlags("", k8sconfig+"/kubelet.kubeconfig")
	if err != nil {
		panic(err.Error())
	}
	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalln(err)
	} else {
		log.Println("Connect k8s success.")
	}

	stopper := make(chan struct{})
	defer close(stopper)

	//初始化informer
	factory := informers.NewSharedInformerFactory(clientset, time.Second*3)
	//nodeInformer := factory.Core().V1().Nodes()
	podInformer := factory.Core().V1().Pods()
	podLister := podInformer.Lister()

	//nodeInformerLister := factory.Core().V1().Nodes().Lister()
	//defer runtime.HandleCrash()

	// 启动 informer，list & watch
	go factory.Start(stopper)

	//从apiserver同步资源，即list
	if !cache.WaitForCacheSync(stopper, podInformer.Informer().HasSynced) {
		runtime.HandleError(fmt.Errorf("Timed out waiting for caches to sync"))
		return
	}

	podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    onAdd,
		UpdateFunc: onUpdate,
		DeleteFunc: onDelete,
	})

	// 声明全局变量
	TaskListChan = make(chan *list.List, 10000)
	clusterId = os.Getenv("CLUSTER_ID")
	waiter := sync.WaitGroup{} //创建sync.WaitGroup{}，保证所有go Routine结束，主线程再终止。
	waiter.Add(1)
	// 初始化接收端Server
	go grpcServer(&waiter) //启动Server Go routine
	// 查询pod状态，并启动发送端client协程
    for i :=0; i < 1000; i++ {
		waiter.Add(1)
		go func() {
			//log.Println("checking out pod's status.")
			defer waiter.Done()
			TaskListRegisterMap := make(map[string]*list.List)
			for {
				newWorkflowTaskList := list.New()
				timeout := time.After(time.Second)

				select {
				case newWorkflowTaskList = <-TaskListChan:
					//log.Println("Received channel's data.")
				case <- timeout:
					//log.Println("Waiting timeout.")
				}

				updateTaskListRegisterMap(TaskListRegisterMap,newWorkflowTaskList)

				getPodsStatus(TaskListRegisterMap, podLister,clientset)
			}
		}()
	}

	<-stopper
	waiter.Wait()
}

// 任务的类
type Task struct {
	workflowid  string
	schedulerip string
	taskid      int32
	podName     string
	state       string
}
func recoverEmptyMapPanic() {
	if r := recover(); r!= nil {
		log.Println("recovered from ", r)
	}
}
// 接收gRPC传来的TaskList，更新旧的键值对或建立新的键值对

func updateTaskListRegisterMap(TaskListRegisterMap map[string]*list.List,newWorkflowTaskList *list.List)  {


	//var TaskListResgisterMap map[string]*list.List
	//defer recoverEmptyMapPanic()
	//log.Printf("newWorkflowTaskList's length is:%d.\n",newWorkflowTaskList.Len())
	if newWorkflowTaskList.Len() ==0 {
		//log.Println("This go routine newWorkflowTaskList is null.")
        return
	}
	workflowid := newWorkflowTaskList.Front().Value.(Task).workflowid
	//log.Printf("Recieved workflowid is:%s.\n",workflowid)
	//log.Printf("workflowid:%s.\n",workflowid)
    //var TaskListRegisterMap map[string]*list.List
	if mapItem, ok := TaskListRegisterMap[workflowid]; ok {
		//registedTaskList := TaskListRegisterMap[workflowid]
		mapItem.PushBackList(newWorkflowTaskList)
		TaskListRegisterMap[workflowid] = mapItem
		//log.Printf("push back newWorkflowTaskList successful,it's length is:%d.\n",newWorkflowTaskList.Len())
		//log.Printf("append map item's key is:%s.\n",workflowid)
	} else {
		TaskListRegisterMap[workflowid] = newWorkflowTaskList
		//log.Printf("Insert map item's key is:%s.\n",workflowid)
	}
	//fmt.Println("-----------------------------")
	//for _, value := range TaskListRegisterMap {
	//
	//	//fmt.Println("key:",key)
	//	//fmt.Println("value:",value)
	//	var taskElement *list.Element
	//	for taskIndex := value.Front(); taskIndex != nil; taskIndex = taskElement {
	//		taskElement = taskIndex.Next()
	//		//log.Printf("Task-workflowID: %v", taskIndex.Value.(Task).workflowid)
	//		//log.Printf("Task-IP: %v", taskIndex.Value.(Task).schedulerip)
	//		log.Printf("xxx-Recevice Task-ID: %v\n", taskIndex.Value.(Task).podName)
	//		//log.Printf("Task-State: %v", taskIndex.Value.(Task).state)
	//	}
	//
	//}
	//log.Printf("TaskListRegisterMap's length is:%d.\n", len(TaskListRegisterMap))
}
func recoverDeletePodFail() {
	if r := recover(); r!= nil {
		log.Println("recovered from ", r)
	}
}
// 遍历Map，双重遍历TaskList，逐个Task查询其状态
func getPodsStatus(TaskListRegisterMap map[string]*list.List, podLister v1.PodLister,clientService *kubernetes.Clientset) {
	defer recoverDeletePodFail()
	//log.Println("Enter getPodsStatus function.********************")
	for workflowid, registedTaskList := range TaskListRegisterMap {

			returnedTaskList := list.New()
		    //log.Printf("workflowid:%s\n",workflowid)
			//var taskElement *list.Element
			for taskIndex := registedTaskList.Front(); taskIndex != nil; taskIndex = taskIndex.Next() {
				// 访问informer缓存
				taskid := clusterId +"-"+ taskIndex.Value.(Task).podName
				//拆分taskId,检测此任务pod是否为调度器又一次发送的执行failed的任务pod
				//taskNameSplit := strings.Split(taskid,"-")
				//if taskNameSplit[len(taskNameSplit)-1] == "1" {
				//	log.Printf("workflowid:%s,taskid:%s\n",workflowid,taskid)
				//}
				//log.Printf("workflowid:%s,taskid:%s\n",workflowid,taskid)
				pod, err := podLister.Pods(workflowid).Get(taskid)
				if err != nil {
                    continue
					//panic(err)
				}
				if pod.Status.Phase == "Succeeded" || pod.Status.Phase == "Failed" {
					var temp Task
					temp = taskIndex.Value.(Task)
					podState := pod.Status.Phase
					//fmt.Println("taskPodName: ",taskIndex.Value.(Task).podName, "'s state is: ",podState)
					temp.state = string(podState)
					//delete pods with Succeeded state or Failed state
					trackerSema <- 1
					err = clientService.CoreV1().Pods(workflowid).Delete(pod.Name, &metav1.DeleteOptions{})
					<- trackerSema
					if err != nil {
						log.Println(err)
						//由于访问etcd timeout,判断该任务pod是否已经删除成功
						time.Sleep(10 * time.Second)
						//podObject, err := clientService.CoreV1().Pods(pod.Namespace).Get(pod.Name,metav1.GetOptions{})
						log.Println("Obtain from Informer after 10 seconds.")
						_, err := podLister.Pods(pod.Namespace).Get(pod.Name)
						if err != nil {
							log.Println(err)
							//if len(podObject.Name) == 0 {
							log.Printf("Not find this task pod: %v, and delay 5 seconds.\n", pod.Name)
							time.Sleep(5 * time.Second)
							//podOb, err := clientService.CoreV1().Pods(pod.Namespace).Get(pod.Name,metav1.GetOptions{})
							_, err := podLister.Pods(pod.Namespace).Get(pod.Name)
							log.Println("Obtain this task pod from Informer again.")
							if err != nil { //说明etcd不存在此任务pod，已经被删除
								log.Println(err)
								log.Printf("Not find this task pod again,the task pod is already deleted.")
								//returnedTaskList.PushBack(temp)
								//deletedPodNum++
								//log.Printf("This is the %dth deleted workflow task pod.\n",deletedPodNum)
								//registedTaskList.Remove(taskIndex)
							} else { //说明该任务Pod没有被删除
								log.Printf("Deleting pod failed. The task pod is also exist: %v.\n", pod.Name)
								continue
							}

						} else {
							time.Sleep(10 * time.Second)
							////从etcd重新获取一次
							//_, err = clientService.CoreV1().Pods(pod.Namespace).Get(pod.Name,metav1.GetOptions{})
							_, err := podLister.Pods(pod.Namespace).Get(pod.Name)
							if err != nil { //从Informer读取不到此任务pod，说明被删除
								log.Println(err)
								log.Printf("Not find this task pod again after 10 seconds,the task pod is already deleted.")
							} else { ////从Informer又一次读取此任务pod，说明存在于etcd
								log.Printf("Deleting pod failed. This task is exist in Informer, %v.\n", pod.Name)
								//continue
							}
						}
					}
					returnedTaskList.PushBack(temp)
					log.Printf("Deleted podName: %s, podStatus: %s.\n", pod.Name, pod.Status.Phase)
					deletedPodNum++
					log.Printf("This is the %dth deleted workflow task pod.\n",deletedPodNum)
					registedTaskList.Remove(taskIndex)
				}
			}
			if registedTaskList.Len() == 0 {
				delete(TaskListRegisterMap, workflowid)
			}
			//}else{
			//	TaskListRegisterMap[workflowid] = registedTaskList
			//}
			if returnedTaskList.Len() > 0 {
				//fmt.Println("returnedTakList",returnedTaskList.Front().Value
				workflowID := returnedTaskList.Front().Value.(Task).workflowid
				//log.Println("workflowID:",workflowID)
				//log.Println("returnedTaskList:",returnedTaskList)
				grpcClientRequest(workflowID, returnedTaskList)
				//fmt.Println("grpcClientRequest over")
			}
		//return TaskListRegisterMap
	}
}
func onAdd(obj interface{}) {
	//pod := obj.(*corev1.Pod)
	//fmt.Println("add a pod:", pod.Name)
}
func onUpdate(old interface{}, current interface{}) {

//	log.Println("updating..............")
}
func onDelete(obj interface{}) {
	//pod := obj.(*corev1.Pod)
	//log.Println("delete a pod:", pod.Name)
}

func homeDir() string {
	if h := os.Getenv("HOME"); h != "" {
		return h
	}
	return os.Getenv("USERPROFILE") // windows
}

func (s *server) InputTaskState(ctx context.Context, in *pb.TaskStateRequest) (*pb.TaskStateReply, error) {
	//log.Println("receiving data from scheduler pods.")
	//var taskList list.List
	taskList := list.New()
	////log.Printf("Task-workflowID: %v\n", taskList.Front().Value.(Task).workflowid)
	TaskListChan <- buildTaskList(taskList, in)
	//log.Println(taskList.Front())
	//log.Println(taskList.Len())
	//以下输出可注释掉，仅为本地测试用，但注释后记得注释相应import
	//var taskElement *list.Element
	//for taskIndex := taskList.Front(); taskIndex != nil; taskIndex = taskElement {
	//	taskElement = taskIndex.Next()

		//log.Printf("Task-workflowID: %v", taskIndex.Value.(Task).workflowid)
		//log.Printf("Task-IP: %v", taskIndex.Value.(Task).schedulerip)
		//log.Printf("Recevice Task-ID: %v\n", taskIndex.Value.(Task).podName)
		//log.Printf("Task-State: %v", taskIndex.Value.(Task).state)
	//}
	return &pb.TaskStateReply{Accept: 1}, nil
}

func buildTaskList(tasklist *list.List, in *pb.TaskStateRequest) *list.List {
	for i := 0; i < len(in.NodeState); i++ {
		var task Task
		task.workflowid = in.WorkflowID
		task.schedulerip = in.IP
		task.taskid = in.NodeState[i].TaskID
		//var s string = strconv.Itoa(int(in.NodeState[i].TaskPodName))
		var s string = in.WorkflowID+"-"+in.NodeState[i].TaskPodName
		task.podName = s
		task.state = "Uncompleted"

		tasklist.PushBack(task)
		//fmt.Printf("task.wfID:%s, task.schedulerIp:%s, task.taskId:%s, task.state:%s.\n",task.workflowid,
		//	task.schedulerip,task.podName,task.state)
	}
	return tasklist
}

//func buildTaskList(tasklist *list.List, in *pb.TaskStateRequest)  {
//	for i := 0; i < len(in.NodeState); i++ {
//		var task Task
//		task.workflowid = in.WorkflowID
//		task.schedulerip = in.IP
//		task.taskid = in.NodeState[i].TaskID
//		//var s string = strconv.Itoa(int(in.NodeState[i].TaskPodName))
//		var s string = in.WorkflowID+"-"+in.NodeState[i].TaskPodName
//		task.podName = s
//		task.state = "Uncompleted"
//
//		tasklist.PushBack(task)
//		//fmt.Printf("task.wfID:%s, task.schedulerIp:%s, task.taskId:%s, task.state:%s.\n",task.workflowid,
//		//	task.schedulerip,task.podName,task.state)
//	}
//}

func recoverGetStateFail() {
	if r := recover(); r!= nil {
		log.Println("recovered from ", r)
	}
}
func grpcClientRequest(id string, TaskList *list.List) {
	//golang异常处理机制，利用recover捕获error
	defer recoverGetStateFail()
	//t1 := time.Now()
	//log.Println("Beginning of a go func: ", t1)
	// Set up a connection to the server.
	log.Println("Go send request to Workflow-Scheduler-", id)

	ip := TaskList.Front().Value.(Task).schedulerip
	log.Printf("Sent to ip: %v", ip)

	//Dial to ip
	ip = ip + ":6060"
	conn, err := grpc.Dial(ip, grpc.WithInsecure())
	//fmt.Println("conn: ", conn)
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	//fmt.Println("1 ready?")
	defer conn.Close()
	c := tb.NewSchedulerClient(conn)

	//fmt.Println("2 ready?")
	//Init a NodeState[]
	totalNum := TaskList.Len()
	var requestNodeState []*tb.TaskStateRequest_NodesState
	requestNodeState = make([]*tb.TaskStateRequest_NodesState, totalNum)

	// init and struct a NodeState{}
	//fmt.Println("3 ready?")
	i := 0
	for e := TaskList.Front(); e != nil; e = e.Next() {
		//for _,task := range TaskList{
		var taskInList Task
		taskInList = e.Value.(Task)
		nodeState := &tb.TaskStateRequest_NodesState{}

		nodeState.TaskID = uint32(taskInList.taskid)

		nodeState.TaskPodName = " "
		nodeState.TaskState = taskInList.state

		requestNodeState[i] = nodeState
		i = i + 1
	}

	//Struct a request
	//fmt.Println("4 ready?")
	request := &tb.TaskStateRequest{
		WorkflowID: id,
		NodeState:  requestNodeState,
		IP:         port,
	}

	//fmt.Println("5 ready?")
	if len(os.Args) > 1 {
		id = os.Args[1]
	}

	//fmt.Println("6 ready?")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	r, err := c.InputTaskState(ctx, request)
	if err != nil {
		//panic(err)
		log.Println("context deadline exceeded.")
		return
	}
	log.Printf("Reported the state: %v", r.GetAccept())
	//t2 := time.Now()
	//log.Println("Ending of a go func: ", t2)
}
