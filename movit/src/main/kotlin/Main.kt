package org.alexiscrack3

import okhttp3.HttpUrl
import okhttp3.OkHttpClient
import okhttp3.Request
import okio.IOException
import org.apache.beam.runners.direct.DirectRunner
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.io.Read
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

class ApiFetcher: DoFn<String, String>() {
    @ProcessElement
    fun processElement(@Element line: String, out: OutputReceiver<String?>) {
        val apiKey = System.getenv("API_KEY") ?: throw IllegalStateException("API_KEY environment variable not set")
        val apiSecret = System.getenv("API_SECRET") ?: throw IllegalStateException("API_SECRET environment variable not set")
        val client = OkHttpClient()
        val url = HttpUrl.Builder()
            .scheme("https")
            .host("runsignup.com")
            .addPathSegment("rest")
            .addPathSegment("races")
            .addQueryParameter("api_key", apiKey)
            .addQueryParameter("api_secret", apiSecret)
            .addQueryParameter("format", "json")
            .build()
        val request = Request.Builder()
            .url(url)
            .get()
            .build()

        val response = client.newCall(request).execute()
        if (!response.isSuccessful) {
            throw IOException("Unexpected code ${response.code}")
        }

        val data = response.body?.string()
        out.output(data)
    }
}

fun main() {
    val options = PipelineOptionsFactory.create()
    options.runner = DirectRunner::class.java
    val p = Pipeline.create(options)

    val input: PCollection<String> = p.apply(TextIO.read().from("input.txt"))
//    val input: PCollection<String> = p.apply(Create.of(LINES)).setCoder(StringUtf8Coder.of())

//    val transformation: ParDo.SingleOutput<String, String> = ParDo.of(LineFormatter())
    val transformation: ParDo.SingleOutput<String, String> = ParDo.of(ApiFetcher())
    val output: PCollection<String> = input.apply(transformation)
    output.apply("WriteToText", TextIO.write().to("build/myfile").withSuffix(".txt"));


    p.run()
}