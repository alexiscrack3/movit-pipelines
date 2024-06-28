package org.alexiscrack3

import org.apache.beam.runners.direct.DirectRunner
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PCollection


val LINES: List<String> = mutableListOf(
    "To be, or not to be: that is the question: ",
    "Whether 'tis nobler in the mind to suffer ",
    "The slings and arrows of outrageous fortune, ",
    "Or to take arms against a sea of troubles, "
)

class LineFormatter: DoFn<String, String>() {
    @ProcessElement
    fun processElement(@Element line: String, out: OutputReceiver<String?>) {
        out.output(line.uppercase())
    }
}


fun main() {
    val options = PipelineOptionsFactory.create()
    options.runner = DirectRunner::class.java
    val p = Pipeline.create(options)

    val input: PCollection<String> = p.apply(Create.of(LINES)).setCoder(StringUtf8Coder.of())

    val transformation: ParDo.SingleOutput<String, String> = ParDo.of(LineFormatter())
    val output = input.apply(transformation)
    output.apply("WriteToText", TextIO.write().to("myfile").withSuffix(".txt"));


    p.run()
}