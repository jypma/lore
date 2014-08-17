package net.ypmania.io

import java.io.File
import java.io.FileOutputStream
import java.nio.file.Paths
import java.nio.file.StandardOpenOption._

import scala.concurrent.duration._

import org.scalatest.Matchers
import org.scalatest.WordSpecLike

import akka.actor.ActorSystem
import akka.actor.Props
import akka.testkit.ImplicitSender
import akka.testkit.TestKit
import akka.util.ByteString

class FileActorSpec extends TestKit(ActorSystem("Test")) with ImplicitSender 
                    with WordSpecLike with Matchers {
  trait Fixture {
    val file = File.createTempFile("FileActorSpec", ".txt")
    def content = "Helloworld"
    private val out = new FileOutputStream(file)
    out.write(content.getBytes)
    out.close
    
    val actor = system.actorOf(Props(
        new FileActor(Paths.get(file.getAbsolutePath), Seq(READ, WRITE, CREATE))))
  }
  
  "A FileActor" should {
    "return a part of the file if requested" in new Fixture {
       actor ! FileActor.Read(0, 5)
       val read = expectMsgType[FileActor.ReadCompleted]
       read.bytes should be (ByteString("Hello"))
    }
    
    "return available bytes if reading from within file to beyond end of file" in new Fixture {
       actor ! FileActor.Read(5, 10)
       val read = expectMsgType[FileActor.ReadCompleted]
       read.bytes should be (ByteString("world"))
    }
  
    "Return no bytes if reading from beyond end of file" in new Fixture {
       watch(actor)
       actor ! FileActor.Read(10, 1)
       val read = expectMsgType[FileActor.ReadCompleted]
       read.bytes should be (ByteString.empty)
    }
  } 
}