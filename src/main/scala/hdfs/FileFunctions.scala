package hdfs

import java.io.{BufferedReader, InputStreamReader}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

class FileFunctions() {
  val conf = new Configuration()
  var fileSystem:FileSystem = FileSystem.get(conf)
  val path:Path=new Path("/user/fall2019/aman")
  val subPath=new Path("/user/fall2019/aman/stm")

   def printUriAndPwd():Unit={
     println("Effective URI With a Configuration object of default values:\n"+fileSystem.getUri+"\n")
     println("Current working directory With a Configuration object of default values:\n"+fileSystem.getWorkingDirectory+"\n")
     effectiveUriAndPwd()
   }

  def effectiveUriAndPwd():Unit={
    conf.addResource(new Path("/Users/apple/opt/hadoop-2.7.3/etc/cloudera/core-site.xml"))
    conf.addResource(new Path("/Users/apple/opt/hadoop-2.7.3/etc/cloudera/hdfs-site.xml"))
    fileSystem = FileSystem.get(conf)
    println("Effective URI after connected to the cluster:\n"+fileSystem.getUri+"\n")
    println("Current working directory:\n"+fileSystem.getWorkingDirectory+"\n")
    listContent()
  }

def listContent():Unit= {
  println("Content of the /user/fall2019:")
  fileSystem.listStatus(new Path("/user/fall2019")).foreach(println)
  println()
  findFolder()
}

  def findFolder():Unit={
  if(fileSystem.exists(path)) {
    println("I found my folder\n")
    deleteFolder()
  }
  else
    println("I failed in the previous practice\n")
  }

  def deleteFolder():Unit={
    if(fileSystem.delete(path)) {
      println("The folder is deleted\n")
      checkFolderDeleted()
    } else
      println("The folder is not deleted\n")

  }

  def checkFolderDeleted():Unit={
    if(fileSystem.exists(path))
      println("Folder is Not Deleted!\n")
    else {
      println("Folder is Successfully Deleted!\n")
      createFolder()
    }
  }

  def createFolder():Unit={
    fileSystem.mkdirs(path)
    println("Folder is successfully created!\n"+fileSystem.resolvePath(path)+"\n")
    checkFolderCreated()
  }

  def checkFolderCreated():Unit= {
    if (fileSystem.exists(path)) {
      println("Folder is Created And Exist at Given Path\n"+fileSystem.resolvePath(path)+"\n")
      createSubFolder()
    } else
      println("Folder is Not Created\n")
  }

  def createSubFolder():Unit={
      fileSystem.mkdirs(subPath)
      println("Sub folder is Successfully created!\n"+fileSystem.resolvePath(subPath)+"\n")
      putFile()
    }

  def putFile():Unit={
    fileSystem.copyFromLocalFile(new Path("/Users/apple/Desktop/stops.txt"),subPath)
    println("File is Successfully Uploaded!\n")
    makeCopy()
  }

  def makeCopy():Unit={
    FileUtil.copy(fileSystem,new Path("/user/fall2019/aman/stm/stops.txt"),fileSystem,new Path("/user/fall2019/aman/stm/stops2.txt"),false,conf)
    println("Copy of File is Created\n")
    renameFile()
  }

  def renameFile():Unit={
    fileSystem.rename(new Path("/user/fall2019/aman/stm/stops2.txt"),new Path("/user/fall2019/aman/stm/stops.csv"))
    println("File is Renamed\n")
    getLines()
  }

  def getLines():Unit={
    println("First five lines of file are:\n")
    val path = new Path("/user/fall2019/aman/stm/stops.csv")
    val bufferedReader:BufferedReader=new BufferedReader(new InputStreamReader(fileSystem.open(path)))
    var line:String=bufferedReader.readLine()
    for(_<- 1 to 5){
      println(line)
      line=bufferedReader.readLine()
    }
  }
}
