module ZipkinTracer
  module Sidekiq
    class ClientMiddleware
      def trace_information(trace_id)
        {
          "trace_id"  => trace_id.trace_id,
          "parent_id" => trace_id.parent_id,
          "span_id"   => trace_id.span_id,
          "sampled"   => trace_id.sampled,
          "flags"     => trace_id.flags
        }
      end

      def call(worker_class, job, _queue, _redis_pool)
        trace_id = TraceGenerator.new.next_trace_id
        TraceContainer.with_trace_id(trace_id) do
          job["zipkin_trace_information"] = trace_information(trace_id)
          if trace_id.sampled?
            ::Trace.tracer.with_new_span(trace_id, "sidekiq") do |span|
              local_endpoint = Trace.default_endpoint
              klass = job["wrapped".freeze] || worker_class
              span.record_tag("job_class",
                              klass,
                              ::Trace::BinaryAnnotation::Type::STRING,
                              local_endpoint)
              yield
            end
          else
            yield
          end
        end
      end
    end
  end
end
