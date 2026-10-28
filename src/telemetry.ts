import { context, propagation, SpanStatusCode, trace } from '@opentelemetry/api';
import { NodeTracerProvider } from '@opentelemetry/sdk-trace-node';
import { ConsoleSpanExporter, SimpleSpanProcessor } from '@opentelemetry/sdk-trace-base';
import { resourceFromAttributes } from '@opentelemetry/resources';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';

const enabled = ['console', 'otlp'].includes(process.env.OTEL_TRACES_EXPORTER ?? 'none');
const provider = enabled ? new NodeTracerProvider({
  resource: resourceFromAttributes({ 'service.name': process.env.OTEL_SERVICE_NAME ?? 'kafka-labs' }),
  spanProcessors: [new SimpleSpanProcessor(process.env.OTEL_TRACES_EXPORTER === 'otlp'
    ? new OTLPTraceExporter() : new ConsoleSpanExporter())]
}) : undefined;
provider?.register();

export async function traced<T>(name: string, topic: string, action: () => Promise<T>, carrier?: Record<string, string>): Promise<T> {
  if (!enabled) return action();
  const parent = carrier ? propagation.extract(context.active(), carrier) : context.active();
  return context.with(parent, () => trace.getTracer('formation').startActiveSpan(name,
    { attributes: { 'messaging.system': 'kafka', 'messaging.destination.name': topic } }, async span => {
      try { return await action(); }
      catch (error) { span.recordException(error as Error); span.setStatus({ code: SpanStatusCode.ERROR }); throw error; }
      finally { span.end(); }
    }));
}
export function traceHeaders(): Record<string, string> {
  const carrier: Record<string, string> = {};
  if (enabled) propagation.inject(context.active(), carrier);
  return carrier;
}
export async function shutdownTelemetry(): Promise<void> { await provider?.shutdown(); }
