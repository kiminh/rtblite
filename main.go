package main

import (
	"flag"
	"fmt"
	"net/http"
	"time"
)

var (
	ConfigFilePath     string
	PrintExampleConfig bool
)

func init() {
	flag.StringVar(&ConfigFilePath, "c", "rtblite.conf", "指定一个配置文件")
	flag.BoolVar(&PrintExampleConfig, "e", false, "打印一份样例配置，你可以将它存为文件后待用 :)")
}

func main() {
	flag.Parse()

	configure := NewConfigure()
	if PrintExampleConfig {
		fmt.Println(configure.String())
		return
	}
	if ConfigFilePath != "" {
		if err := configure.LoadFromFile(ConfigFilePath); err != nil {
			fmt.Println("load from", ConfigFilePath, "failed:", err.Error())
			fmt.Println("using default configure")
		}
	}
	fmt.Println("current active configure:", configure.String())

	rtblite, err := NewRtbLite(configure)
	if err != nil {
		fmt.Println("fail to create server instance:", err.Error())
		return
	}
	if err := rtblite.CacheUpdateLoop(30 * time.Second); err != nil {
		fmt.Println("fail to fetch initial inventory:", err.Error())
		return
	}
	listenOn := configure.HttpAddress
	fmt.Println("server start on %v", listenOn)
	http.HandleFunc("/request", rtblite.Request)        //设定访问的路径
	http.HandleFunc("/impression", rtblite.Impression)  //设定访问的路径
	http.HandleFunc("/click", rtblite.Click)            //设定访问的路径
	http.HandleFunc("/td_postback", rtblite.Conversion) //设定访问的路径
	http.ListenAndServe(listenOn, nil)                  //设定端口和handler
}
