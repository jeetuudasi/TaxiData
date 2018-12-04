import DataProducer.DataProducer
import Stream.Aggregator

object ContextMain {

  def main(args: Array[String]): Unit = {
    val dp = new DataProducer();

    //dp.startProcess();

    val streamAgg = new Aggregator();
    streamAgg.startProcess()
  }

}
