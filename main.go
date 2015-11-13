package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"runtime"
)

var (
	ConfigFilePath      string
	PrintExampleConfig  bool
	UpdateExampleConfig bool
)

func init() {
	flag.StringVar(&ConfigFilePath, "c", "rtblite.conf", "指定一个配置文件")
	flag.BoolVar(&PrintExampleConfig, "e", false, "打印一份样例配置，你可以将它存为文件后待用 :)")
	flag.BoolVar(&UpdateExampleConfig, "u", false, "升级配置文件，你可以将它存为文件后待用 :)")
}

var rank *RankTable

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

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
	if UpdateExampleConfig {
		configBackup := ConfigFilePath + ".bak"
		if err := os.Rename(ConfigFilePath, configBackup); err != nil {
			fmt.Println("fail to backup old conf")
			return
		} else {
			fmt.Println("old conf has been renamed to", configBackup)
		}

		fmt.Println("conf will be updated to:")
		fmt.Println(configure.String())
		if err := configure.SaveToFile(ConfigFilePath); err != nil {
			fmt.Println("update configure file failed")
		} else {
			fmt.Println("done")
		}
		return
	}

	fmt.Println("current active configure:", configure.String())
	rtblite, err := NewRtbLite(configure)
	if err != nil {
		fmt.Println("fail to create server instance:", err.Error())
		return
	}
	if err := rtblite.CacheUpdateLoop(); err != nil {
		fmt.Println("fail to fetch initial inventory:", err.Error())
		return
	}
	rtblite.RunProfiler()
	rtblite.BlockListUpdateLoop()

	listenOn := configure.HttpAddress

	mux := http.NewServeMux()
	mux.HandleFunc("/request", rtblite.Request)        //设定访问的路径
	mux.HandleFunc("/impression", rtblite.Impression)  //设定访问的路径
	mux.HandleFunc("/click", rtblite.Click)            //设定访问的路径
	mux.HandleFunc("/event", rtblite.Conversion)       //设定访问的路径
	mux.HandleFunc("/rank/update", rtblite.UpdateRank) //设定访问的路径
	mux.HandleFunc("/rank", rtblite.GetRank)           //设定访问的路径

	fmt.Println("server start on ", listenOn)

	//	if err := gracehttp.Serve(
	//		&http.Server{
	//			Addr:    listenOn,
	//			Handler: mux},
	//	); err != nil {
	//		fmt.Println(err.Error())
	//	}

	if err := http.ListenAndServe(listenOn, mux); err != nil {
		fmt.Println(err.Error())
	}
}
