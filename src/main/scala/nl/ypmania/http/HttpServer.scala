package nl.ypmania.http

import akka.actor._
import akka.util.{ ByteString, ByteStringBuilder }
import java.net.InetSocketAddress

class HttpServer(port: Int) extends Actor {
  val state = IO.IterateeRef.Map.async[IO.Handle]()(context.dispatcher)
  import HttpIteratees._
 

  override def preStart {
    IOManager(context.system) listen new InetSocketAddress(port)
  }

  def receive = {
    case IO.NewClient(server) =>
      val socket = server.accept()
      state(socket) flatMap (_ => processRequest(socket))

    case IO.Read(socket, bytes) =>
      state(socket)(IO Chunk bytes)

    case IO.Closed(socket, cause) =>
      state(socket)(IO EOF None)
      state -= socket
  }
  
  def processRequest(socket: IO.SocketHandle): IO.Iteratee[Unit] = IO repeat {
    for {
      request <- readRequest
    } yield {
      println(request)
      val rsp = request match {
        case GET("ping" :: Nil, _, headers) =>
          OKResponse(ByteString("<p>pong</p>"),
            request.headers.exists { case Header(n, v) => n.toLowerCase == "connection" && v.toLowerCase == "keep-alive" })
        case req =>
          OKResponse(ByteString("<p>" + req.toString + "</p>"),
            request.headers.exists { case Header(n, v) => n.toLowerCase == "connection" && v.toLowerCase == "keep-alive" })
      }
      socket write OKResponse.bytes(rsp).compact
      println("keep alive: " + rsp.keepAlive)
      if (!rsp.keepAlive) socket.close()
    }
  }
  
  def serve(f:PartialFunction[Request,Unit]) {
    
  }
  
}
