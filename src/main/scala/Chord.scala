import akka.actor._
import scala.math._
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.util.control.Breaks._
import scala.concurrent.duration._
import java.security.MessageDigest


case object Go
case class FirstJoin()
case class SecondJoin()
case class JoinNext(activeNode : ArrayBuffer[Int])
case class UpdateFinger(activeNode : ArrayBuffer[Int])
case class exit()
case class Routing()
case object BeginRoute
case class Route(fromID: Int, toID: Int, hops: Int, visited : ArrayBuffer[Int])
case class RouteFinish(fromID: Int, toID: Int, hops: Int)
case class Failure()

object project3_bonus {
  def main(args: Array[String]) {
    var numNodes: Int = 0;
    var numRequests: Int =0;

    if (args.length != 2) {
      println("No Argument(s)! Using default mode:")
      numNodes = 1024
      numRequests = 10 //Default mode

    } else {
      numNodes = args(0).toInt
      numRequests = args(1).toInt //User Specified Mode
    }
    println(numNodes+" "+numRequests)
    chordmain(numNodes, numRequests)
    //calculate m useful to calculate spaceid


    def chordmain(numNodes: Int, numRequests: Int) {
      val system = ActorSystem("chord")
      val master = system.actorOf(Props(new Master(numNodes, numRequests)), name = "master")
      master ! Go
    }
  }
}

class Master(numNodes: Int, numRequests: Int) extends Actor {
  var ranlist = new ArrayBuffer[Int]()
  var noOfneighbors : Int = ceil(log(numNodes.toDouble) / log(2)).toInt
  var noOfSpace : Int = pow(2,noOfneighbors).toInt
  var numFirstGroup: Int = numNodes
  var i: Int = -1
  //var table = new Array[Array[Array[Double]]](noOfneighbors)
  var activeNode = new ArrayBuffer[Int]()
  var countActive = 0
  var numRouted: Int = 0
  var numHops: Int = 0


  for (i <- 0 until numNodes){
    activeNode += -1
  }

  /*var ipadd : Int = 0
  var ip: String = ""
  var seed1: String = Random.alphanumeric.take(10).mkString
  val sha = MessageDigest.getInstance("SHA-1")
  ip = sha.digest(seed1.getBytes).foldLeft("")((s: String, b: Byte) => s + Character.forDigit((b & 0xf0) >> 4, 16) + Character.forDigit(b & 0x0f, 16))
  ipadd = ip.replaceAll("[a-zA-Z]", "").substring(0, noOfneighbors).toInt
  ranlist += ipadd
  println(ipadd)

  for (i <- 0 until noOfSpace-1) { //Node space form 0 to node id space
    while(ranlist.contains(ipadd)) {
      var seed1: String = Random.alphanumeric.take(10).mkString
      val sha = MessageDigest.getInstance("SHA-1")
      ip = sha.digest(seed1.getBytes).foldLeft("")((s: String, b: Byte) => s + Character.forDigit((b & 0xf0) >> 4, 16) + Character.forDigit(b & 0x0f, 16))
      ipadd = ip.replaceAll("[a-zA-Z]", "").substring(0, noOfneighbors).toInt
    }
    ranlist += ipadd
    println(ipadd)
  }*/

  for (i <- 0 until noOfSpace) { //Node space form 0 to node id space
    ranlist += i
  }

  ranlist = Random.shuffle(ranlist)

  for (i <- 0 until numNodes){
    context.actorOf(Props(new ChordActor(noOfSpace, numRequests, ranlist(i), noOfneighbors)), name = String.valueOf(ranlist(i)))
  }

  def receive = {
    case Go =>
      println("Join Begins...")
      activeNode(0) = ranlist(0)
      countActive += 1
      context.system.actorSelection("/user/master/" + ranlist(0)) ! FirstJoin()

    case SecondJoin() =>
      if(countActive>=numNodes){
        self ! Routing()
      }else{
        Thread.sleep(50)
        activeNode += ranlist(countActive)
        //println("node added: " + ranlist(countActive))
        context.system.actorSelection("/user/master/" + ranlist(countActive)) ! JoinNext(activeNode)
        countActive += 1
      }

    case Failure() =>
        val rand = new Random(System.currentTimeMillis());
        val random_index = rand.nextInt(activeNode.length);
        val result = activeNode(random_index);
        context.system.actorSelection("/user/master/" + result) ! PoisonPill
        activeNode -= result

    case Routing()=>
      println("Join Finished!")
      Failure()
      println("Routing Begins...")
      for (i <- 0 until activeNode.length) {
        context.system.actorSelection("/user/master/"+activeNode(i)) ! BeginRoute
      }

    case RouteFinish(fromID, toID, hops) =>
      numRouted += 1
      numHops += hops
      for (i <- 1 to 10)
        if (numRouted == numNodes * numRequests * i / 10)
          println(i + "0% Routing Finished...")

      if (numRouted == numNodes * numRequests) {
        println("Number of Total Routes: " + numRouted)
        println("Number of Total Hops: " + numHops)
        println("Average Hops Per Route: " + numHops.toDouble / numRouted.toDouble)
        context.system.shutdown()
      }

  }

}

class ChordActor(noOfSpace: Int, numRequests: Int, id: Int, noOfneighbors: Int) extends Actor {

  import context._

  val myID = id
  var start : Double = -1
  var end : Double = -1
  var BeginInterval : Double = -1
  var EndInterval : Double = -1
  var successor : Double = -1
  val row : Int = noOfneighbors
  val column : Int = 4
  var fingertable = Array.ofDim[Double](row,column)
  var visited = new ArrayBuffer[Int]()
  var failureFlag : Boolean = true

  var count : Int = 0

  var i = 0
  for (i <- 0 until noOfneighbors){
    fingertable(i)(0) = start
    fingertable(i)(1) = start
    fingertable(i)(2) = EndInterval
    fingertable(i)(3) = successor
  }


  def receive = {
    case Go =>
      println("Join Begins...")
    case FirstJoin() =>
      if(myID<noOfSpace/2){
        for (i <- 0 until noOfneighbors) {
          var end : Double = -1;
          var start : Double = myID+(pow(2,noOfneighbors)/pow(2,(i+1)))
          if(i==0){
            end = myID
          }else{
            end = fingertable(i-1)(1)
          }

          fingertable(i)(0) = start
          fingertable(i)(1) = start
          fingertable(i)(2) = end
          fingertable(i)(3) = myID
        }


      }
      else{
        for (i <- 0 until noOfneighbors) {
          var start : Double = myID+(pow(2,noOfneighbors)/pow(2,(i+1)))
          if(start>=noOfSpace){
            start = start - noOfSpace
          }
          if(i==0){
            end = myID
          }else{
            end = fingertable(i-1)(1)
          }
          fingertable(i)(0) = start
          fingertable(i)(1) = start
          fingertable(i)(2) = end
          fingertable(i)(3) = myID
        }

      }
      fingertable.reverse
      //println("Node added : "+myID)
      //table(0) = fingertable
      /*for (i <- 0 until noOfneighbors) {
        println(myID+" : "+fingertable(i)(0).toInt+" "+ fingertable(i)(1).toInt+" "+fingertable(i)(2).toInt+" "+fingertable(i)(3).toInt)
      }*/
      //table(0) = fingertable

      sender ! SecondJoin()



    case JoinNext(activeNode : ArrayBuffer[Int]) =>
      if(myID<noOfSpace/2){
        for (i <- 0 until noOfneighbors) {
          var end : Double = -1;
          var start : Double = myID+(pow(2,noOfneighbors)/pow(2,(i+1)))
          if(i==0){
            end = myID
          }else{
            end = fingertable(i-1)(1)
          }

          fingertable(i)(0) = start
          fingertable(i)(1) = start
          fingertable(i)(2) = end
          if(activeNode.contains(start)){
            fingertable(i)(3) = start
          }
          else{
            breakable {
              for (j <- 1 to noOfSpace) {
                if (activeNode.contains((start + j) % noOfSpace)) {
                  fingertable(i)(3) = (start + j) % noOfSpace
                  break
                }
              }
            }
          }
        }

      }
      else{
        for (i <- 0 until noOfneighbors) {
          var start : Double = myID+(pow(2,noOfneighbors)/pow(2,(i+1)))
          if(start>=noOfSpace){
            start = start - noOfSpace
          }
          if(i==0){
            end = myID
          }else{
            end = fingertable(i-1)(1)
          }
          fingertable(i)(0) = start
          fingertable(i)(1) = start
          fingertable(i)(2) = end
          if(activeNode.contains(start)){
            fingertable(i)(3) = start
          }
          else{
            breakable {
              for (j <- 1 to noOfSpace) {
                if (activeNode.contains((start + j) % noOfSpace)) {
                  fingertable(i)(3) = (start + j) % noOfSpace
                  break
                }
              }
            }
          }
        }

      }
      fingertable.reverse
      /*for (i <- 0 until noOfneighbors) {
        println(myID+" : "+fingertable(i)(0).toInt+" "+ fingertable(i)(1).toInt+" "+fingertable(i)(2).toInt+" "+fingertable(i)(3).toInt)
      }*/
      //table(2) = fingertable
      for (i <- 0 until activeNode.length){
        context.system.actorSelection("/user/master/" + activeNode(i)) ! UpdateFinger(activeNode)
      }


      sender ! SecondJoin()

    case UpdateFinger(activeNode)=>
      for(i <- 0 until noOfneighbors){
        if(activeNode.contains(fingertable(i)(0))){
          fingertable(i)(3) = fingertable(i)(0)
        }else{
          breakable {
            for (j <- 1 to noOfSpace) {
              if (activeNode.contains((fingertable(i)(0) + j) % noOfSpace)) {
                fingertable(i)(3) = (fingertable(i)(0) + j) % noOfSpace
                break
              }
            }
          }
        }
      }
    /*for (i <- 0 until noOfneighbors) {
      println(myID+" : "+fingertable(i)(0).toInt+" "+ fingertable(i)(1).toInt+" "+fingertable(i)(2).toInt+" "+fingertable(i)(3).toInt)
    }*/

    case BeginRoute =>
      for (i <- 0 until numRequests)
        context.system.scheduler.scheduleOnce(1000 milliseconds, self, Route(myID, Random.nextInt(noOfSpace-1), 0,visited))

    case Route(fromID, toID, hops, visited) =>
      var visitedlist = new ArrayBuffer[Int]()

      visitedlist = visited
      if(hops==0){
        visitedlist += fromID
      }

      var flag : Boolean = true
      var flag1 : Boolean = true
      var flag2 : Boolean = true
      //println("myid : "+myID+" toid : "+toID+" fromid : "+fromID)
      if (myID == toID) {
        //println("Exiting as my id is same as to id "+ toID+" "+myID)
        context.parent ! RouteFinish(fromID, toID, hops)
      }
      else {
        //println("going to loop "+ toID+" "+myID)
        breakable{
          for(i <- 0 until noOfneighbors){
            if(fingertable(i)(0).toInt==toID){
              flag = false
              //println("direct neighbor detected "+ toID+" "+myID)
              context.parent ! RouteFinish(fromID, toID, hops + 1)
              break
            }
          }
        }
        if(flag) {
          //println("Direct neighbor is not found "+ toID+" "+myID)
          if (myID <= noOfSpace / 2) {
            //println("contains in first half : "+ toID+" "+myID)
            breakable {
              for (i <- 0 until noOfneighbors) {
                if ((fingertable(i)(1).toInt < toID) && (fingertable(i)(2).toInt > toID)) {
                  //println("1. Interval condition 1st half"+ toID+" "+myID)
                  flag1 = false

                  if(visitedlist.contains(fingertable(i)(3).toInt)){
                    context.parent ! RouteFinish(fromID, toID, hops + 1)
                  }else {
                    visitedlist += fingertable(i)(3).toInt
                    context.system.actorSelection("/user/master/" + fingertable(i)(3).toInt) ! Route(fromID, toID, hops + 1, visitedlist)
                  }
                  break

                }
                else if((fingertable(i)(1).toInt < toID) && (fingertable(i)(2).toInt==myID)){
                  flag1 = false;
                  //println("1. Interval condition 1st half"+ toID+" "+myID)

                  if(visitedlist.contains(fingertable(i)(3).toInt)){
                    context.parent ! RouteFinish(fromID, toID, hops + 1)
                  }else {
                    visitedlist += fingertable(i)(3).toInt
                    context.system.actorSelection("/user/master/" + fingertable(i)(3).toInt) ! Route(fromID, toID, hops + 1, visitedlist)

                    break
                  }
                }
              }
              if(flag1){
                //println("In default loop "+ toID+" "+myID+" "+fingertable(0)(3).toInt)

                if(visitedlist.contains(fingertable(i)(3).toInt)){
                  context.parent ! RouteFinish(fromID, toID, hops + 1)
                }else {
                  visitedlist += fingertable(0)(3).toInt
                  context.system.actorSelection("/user/master/" + fingertable(0)(3).toInt) ! Route(fromID, toID, hops + 1, visitedlist)
                }

              }
            }

          }
          else{
            //println("contains in second half : "+ toID+" "+myID)
            breakable {
              for (i <- 0 until noOfneighbors) {
                if ((fingertable(i)(1).toInt < toID) && (fingertable(i)(2).toInt > toID)) {
                  //println("2. first interval " + toID + " " + myID)
                  flag2 = false

                  if(visitedlist.contains(fingertable(i)(3).toInt)){
                    context.parent ! RouteFinish(fromID, toID, hops + 1)
                  }else {
                    visitedlist += fingertable(i)(3).toInt
                    context.system.actorSelection("/user/master/" + fingertable(i)(3).toInt) ! Route(fromID, toID, hops + 1, visitedlist)
                  }

                  break
                }
              }
              if(flag2){
                //println("Default loop for bigger half "+ toID+" "+myID)
                if(visitedlist.contains(fingertable(i)(3).toInt)){
                  context.parent ! RouteFinish(fromID, toID, hops + 1)
                }else {
                  visitedlist += fingertable(1)(3).toInt
                  context.system.actorSelection("/user/master/" + fingertable(1)(3).toInt) ! Route(fromID, toID, hops + 1, visitedlist)
                }
              }
            }
          }
        }

      }
  }
}

