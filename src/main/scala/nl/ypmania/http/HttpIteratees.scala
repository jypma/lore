package nl.ypmania.http

//#imports
import akka.actor._
import akka.util.{ ByteString, ByteStringBuilder }
import java.net.InetSocketAddress
//#imports

//#request-class
case class Header(name: String, value: String)

trait Request {
  val path: List[String]
  val query: Map[String,String]
  val headers: List[Header]
}

case class GET(path: List[String], query: Map[String,String], headers: List[Header]) extends Request
case class PUT(path: List[String], query: Map[String,String], body: Option[ByteString], headers: List[Header])  extends Request
case class POST(path: List[String], query: Map[String,String], body: Option[ByteString], headers: List[Header])  extends Request
case class DELETE(path: List[String], query: Map[String,String], headers: List[Header])  extends Request

//#request-class

//#constants
object HttpConstants {
  val SP = ByteString(" ")
  val HT = ByteString("\t")
  val CR = ByteString("\r")
  val LF = ByteString("\n")
  val CRLF = ByteString("\r\n")
  val COLON = ByteString(":")
  val PERCENT = ByteString("%")
  val PATH = ByteString("/")
  val QUERY = ByteString("?")
  val EQUALS = ByteString("=")
  val AMP = ByteString("&")
}
//#constants

//#read-request
object HttpIteratees {
  import HttpConstants._

  def dropCR (bytes: ByteString) = {
    println("dropCR: " + bytes + " / " + bytes.utf8String);
    if (bytes.last == 13) bytes.slice(0, bytes.size - 1); else bytes;
  }
  
  def readRequest =
    for {
      requestLine <- readRequestLine
      (meth, (path, query), httpver) = requestLine
      headers <- readHeaders
      body <- readBody(headers)
    } yield {
      meth match {
        case "GET" => GET(path, query, headers) 
        case "PUT" => PUT(path, query, body, headers) 
        case "POST" => POST(path, query, body, headers) 
        case "DELETE" => DELETE(path, query, headers)
        case _ => sys.error ("Unknown method: " + meth)
      }
    }
  //#read-request

  //#read-request-line
  def ascii(bytes: ByteString): String = bytes.decodeString("US-ASCII").trim

  def readRequestLine = {
    println("readRequestLine")
    for {
      meth <- IO takeUntil SP
      uri <- readRequestURI
      delim <- IO takeWhile { b => b != 32 && b != 10}
      end <- IO take 1
      httpVerIter = if (end(0) == 10) IO Done ByteString("none"); else IO takeUntil LF
      httpver <- httpVerIter
    } yield (ascii(meth), uri, ascii(dropCR(httpver)))
  //#read-request-line
  }

  //#read-request-uri
  def readRequestURI = {println ("readRequestURI"); new Exception().printStackTrace(); IO peek 1 flatMap {
    case PATH =>
      println("got PATH")
      for {
        path <- readPath
        query <- readQuery
      } yield (path, query)
    case other => sys.error("foo")
  }}
  //#read-request-uri

  //#read-path
  def readPath = {
    def step(segments: List[String]): IO.Iteratee[List[String]] = IO peek 1 flatMap {
      case PATH => IO drop 1 flatMap (_ => readUriPart(pathchar) flatMap (segment => step(segment :: segments)))
      case _ => segments match {
        case "" :: rest => IO Done rest.reverse
        case _          => IO Done segments.reverse
      }
    }
    step(Nil)
  }
  //#read-path

  //#read-query
  def readQuery: IO.Iteratee[Map[String,String]] = {
    def readNameValue(q: Map[String,String]): IO.Iteratee[Map[String,String]] = {
      readUriPart(querynamechar) flatMap { name =>
        IO take 1 flatMap {
          case EQUALS => readUriPart(queryvaluechar) flatMap { value =>
            IO take 1 flatMap {
              case AMP => readNameValue (q + (name -> value))
              case _ => IO Done q + (name -> value)
            }
          }
          case AMP => readNameValue (q + (name -> ""))
          case _ => IO Done q + (name -> "")
        }
      }      
    }
    
    IO peek 1 flatMap {
      case QUERY => IO drop 1 flatMap (_ => readNameValue(Map.empty))
      case _     => IO Done Map.empty
    }
  }
  //#read-query
  
  //#read-uri-part
  val alpha = Set.empty ++ ('a' to 'z') ++ ('A' to 'Z') map (_.toByte)
  val digit = Set.empty ++ ('0' to '9') map (_.toByte)
  val hexdigit = digit ++ (Set.empty ++ ('a' to 'f') ++ ('A' to 'F') map (_.toByte))
  val subdelim = Set('!', '$', '\'', '(', ')', '*', '+', ',', ';') map (_.toByte)
  val urichar = alpha ++ digit ++ subdelim ++ (Set(':', '@') map (_.toByte))
  val pathchar = urichar ++ Set('=', '&').map (_.toByte)
  
  val querychar = urichar ++ (Set('/', '?') map (_.toByte))
  val querynamechar = querychar
  val queryvaluechar = querychar ++ (Set('=') map (_.toByte))

  def readUriPart(allowed: Set[Byte]): IO.Iteratee[String] = for {
    str <- IO takeWhile allowed map ascii
    pchar <- IO peek 1 map (_ == PERCENT)
    all <- if (pchar) readPChar flatMap (ch => readUriPart(allowed) map (str + ch + _)) else IO Done str
  } yield all

  def readPChar = IO take 3 map {
    case Seq('%', rest @ _*) if rest forall hexdigit =>
      java.lang.Integer.parseInt(rest map (_.toChar) mkString, 16).toChar
  }
  //#read-uri-part

  //#read-headers
  def readHeaders = {
    def step(found: List[Header]): IO.Iteratee[List[Header]] = {
      IO peek 1 flatMap {
        case LF => IO takeUntil LF flatMap (_ => IO Done found)
        case CR => IO takeUntil LF flatMap (_ => IO Done found)
        case _    => readHeader flatMap (header => step(header :: found))
      }
    }
    step(Nil)
  }

  def readHeader = for {
    name <- IO takeUntil COLON
    value <- IO takeUntil LF flatMap { initial => readMultiLineValue(dropCR(initial)) }
  } yield Header(ascii(name), ascii(value))

  def readMultiLineValue(initial: ByteString): IO.Iteratee[ByteString] = IO peek 1 flatMap {
    case SP => IO takeUntil LF flatMap (bytes => readMultiLineValue(initial ++ dropCR(bytes)))
    case _  => IO Done initial
  }
  //#read-headers

  //#read-body
  def readBody(headers: List[Header]) =
    if (headers.exists(header => header.name == "Content-Length" || header.name == "Transfer-Encoding"))
      IO.takeAll map (Some(_))
    else
      IO Done None
  //#read-body
}

//#ok-response
object OKResponse {
  import HttpConstants.CRLF

  val okStatus = ByteString("HTTP/1.1 200 OK")
  val contentType = ByteString("Content-Type: text/html; charset=utf-8")
  val cacheControl = ByteString("Cache-Control: no-cache")
  val date = ByteString("Date: ")
  val server = ByteString("Server: Akka")
  val contentLength = ByteString("Content-Length: ")
  val connection = ByteString("Connection: ")
  val keepAlive = ByteString("Keep-Alive")
  val close = ByteString("Close")

  def bytes(rsp: OKResponse) = {
    new ByteStringBuilder ++=
      okStatus ++= CRLF ++=
      contentType ++= CRLF ++=
      cacheControl ++= CRLF ++=
      date ++= ByteString(new java.util.Date().toString) ++= CRLF ++=
      server ++= CRLF ++=
      contentLength ++= ByteString(rsp.body.length.toString) ++= CRLF ++=
      connection ++= (if (rsp.keepAlive) keepAlive else close) ++= CRLF ++= CRLF ++= rsp.body result
  }

}
case class OKResponse(body: ByteString, keepAlive: Boolean)
//#ok-response
