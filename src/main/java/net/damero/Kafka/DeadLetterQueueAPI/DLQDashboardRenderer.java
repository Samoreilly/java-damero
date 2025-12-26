package net.damero.Kafka.DeadLetterQueueAPI;

import java.util.List;

/**
 * Utility class to render the DLQ Dashboard using HTMX and Tailwind CSS.
 */
public class DLQDashboardRenderer {

    /**
     * Renders the full dashboard page.
     */
    public static String renderDashboard(DLQResponse response) {
        String topic = response.getTopic() != null ? response.getTopic() : "test-dlq";
        int total = response.getSummary() != null ? response.getSummary().getTotalEvents() : 0;
        int high = response.getSummary() != null ? response.getSummary().getHighSeverityCount() : 0;
        String queriedAt = response.getQueriedAt() != null ? response.getQueriedAt() : "N/A";

        String template = """
                <!DOCTYPE html>
                <html lang="en" data-theme="dark">
                <head>
                    <meta charset="UTF-8">
                    <meta name="viewport" content="width=device-width, initial-scale=1.0">
                    <title>Damero DLQ Dashboard</title>
                    <script src="https://unpkg.com/htmx.org@1.9.10"></script>
                    <script src="https://cdn.tailwindcss.com"></script>
                    <link href="https://cdn.jsdelivr.net/npm/daisyui@4.6.0/dist/full.min.css" rel="stylesheet" type="text/css" />
                    <script>
                        tailwind.config = {
                            theme: {
                                extend: {
                                    colors: {
                                        zinc: {
                                            950: '#09090b',
                                        }
                                    }
                                }
                            }
                        }
                    </script>
                    <style>
                        .htmx-indicator { display: none; }
                        .htmx-request .htmx-indicator { display: block; }
                        .htmx-request.htmx-indicator { display: block; }
                        [hx-disable], [hx-disable] * { cursor: not-allowed !important; }
                        body { background-color: #09090b; }
                    </style>
                </head>
                <body class="bg-zinc-950 min-h-screen font-sans text-zinc-200">
                    <div class="container mx-auto p-4 lg:p-8">
                        <!-- Header & Stats -->
                        <div class="flex flex-col md:flex-row justify-between items-start md:items-center mb-8 gap-6">
                            <div>
                                <div class="flex items-center gap-4">
                                    <h1 class="text-3xl font-bold tracking-tight text-white mb-1">
                                        Damero <span class="text-zinc-500">DLQ</span>
                                    </h1>
                                    <a href="https://github.com/samoreilly/java-damero" target="_blank" class="text-zinc-600 hover:text-white transition-colors">
                                        <svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="lucide lucide-github"><path d="M15 22v-4a4.8 4.8 0 0 0-1-3.5c3 0 6-2 6-5.5.08-1.25-.27-2.48-1-3.5.28-1.15.28-2.35 0-3.5 0 0-1 0-3 1.5-2.64-.5-5.36-.5-8 0C6 2 5 2 5 2c-.3 1.15-.3 2.35 0 3.5A5.403 5.403 0 0 0 4 9c0 3.5 3 5.5 6 5.5-.39.49-.68 1.05-.85 1.65-.17.6-.22 1.23-.15 1.85v4"/><path d="M9 18c-4.51 2-5-2-7-2"/></svg>
                                    </a>
                                </div>
                                <p class="text-zinc-500 font-medium tracking-tight">Enterprise Kafka Resilience Dashboard</p>
                            </div>

                            <div class="stats shadow-sm bg-zinc-900 border border-zinc-800 rounded-xl">
                                <div class="stat px-6 py-4">
                                    <div class="stat-title font-semibold uppercase text-[10px] tracking-wider text-zinc-500">Total Events</div>
                                    <div class="stat-value text-white text-2xl font-bold">{{TOTAL}}</div>
                                </div>

                                <div class="stat px-6 py-4 border-l border-zinc-800">
                                    <div class="stat-title font-semibold uppercase text-[10px] tracking-wider text-red-500/80 text-xs">Critical</div>
                                    <div class="stat-value text-red-500 text-2xl font-bold">{{HIGH}}</div>
                                </div>
                            </div>
                        </div>

                        <!-- Search & Replay Box -->
                        <div class="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-8">
                            <div class="lg:col-span-2 card bg-zinc-900 shadow-sm border border-zinc-800">
                                <div class="card-body p-6">
                                    <h2 class="text-xs font-semibold uppercase tracking-widest text-zinc-500 mb-3">Topic Filter</h2>
                                    <div class="relative w-full">
                                        <div class="absolute inset-y-0 left-4 flex items-center pointer-events-none text-zinc-600">
                                            <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" /></svg>
                                        </div>
                                        <input type="text"
                                               name="topic"
                                               value="{{TOPIC}}"
                                               placeholder="Search by DLQ topic name..."
                                               class="bg-zinc-950 border-zinc-800 text-white input input-bordered input-lg w-full pl-12 font-medium focus:ring-1 focus:ring-zinc-700 transition-all"
                                               hx-get="/dlq/dashboard/table"
                                               hx-target="#dlq-table-container"
                                               hx-trigger="keyup changed delay:300ms, search"
                                               hx-indicator="#loading-indicator">
                                        <span id="loading-indicator" class="htmx-indicator loading loading-spinner loading-md text-zinc-500 absolute right-4 top-1/2 -translate-y-1/2"></span>
                                    </div>
                                </div>
                            </div>

                            <div class="card bg-zinc-900 shadow-sm border border-zinc-800">
                                <div class="card-body p-6 justify-center">
                                    <h2 class="text-xs font-semibold uppercase tracking-widest text-zinc-500 mb-3">Actions</h2>
                                    <button class="btn btn-md bg-white hover:bg-zinc-200 text-black border-none font-bold"
                                            hx-post="/dlq/replay/{{TOPIC}}"
                                            hx-target="#dlq-table-container"
                                            hx-confirm="Are you sure you want to replay all messages in topic '{{TOPIC}}'?"
                                            hx-indicator="#loading-indicator">
                                        <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4 mr-2" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357-2H15" /></svg>
                                        Replay All
                                    </button>
                                </div>
                            </div>
                        </div>

                        <!-- Content Table -->
                        <div id="dlq-table-container" class="bg-zinc-900 rounded-xl shadow-sm border border-zinc-800 overflow-hidden">
                            {{TABLE}}
                        </div>

                        <footer class="mt-12 mb-8 pt-8 border-t border-zinc-800 flex flex-col md:flex-row justify-between items-center gap-4 text-zinc-600 text-[11px] font-medium uppercase tracking-widest">
                            <p>&copy; 2025 Damero Kafka Library</p>
                            <div class="flex items-center gap-4">
                                <p>Synced: {{QUERIED_AT}}</p>
                                <button onclick="window.location.reload()" class="hover:text-white transition-colors uppercase">Refresh</button>
                            </div>
                        </footer>
                    </div>
                    <script>
                        function copyToClipboard(text, el) {
                            navigator.clipboard.writeText(text).then(() => {
                                const original = el.innerHTML;
                                el.innerHTML = 'COPIED';
                                setTimeout(() => el.innerHTML = original, 2000);
                            });
                        }
                    </script>
                </body>
                </html>
                """;

        return template
                .replace("{{TOPIC}}", topic)
                .replace("{{TOTAL}}", String.valueOf(total))
                .replace("{{HIGH}}", String.valueOf(high))
                .replace("{{QUERIED_AT}}", queriedAt)
                .replace("{{TABLE}}", renderTable(response));
    }

    /**
     * Renders just the table portion for partial HTMX updates.
     */
    public static String renderTable(DLQResponse response) {
        StringBuilder sb = new StringBuilder();
        sb.append(
                """
                        <div class="overflow-x-auto">
                            <table class="table table-md w-full border-separate border-spacing-0">
                                <thead>
                                    <tr class="bg-zinc-800/30 text-zinc-500">
                                        <th class="border-b border-zinc-800 py-4 font-bold uppercase text-[10px] tracking-widest pl-8">Severity</th>
                                        <th class="border-b border-zinc-800 py-4 font-bold uppercase text-[10px] tracking-widest">Event Detail</th>
                                        <th class="border-b border-zinc-800 py-4 font-bold uppercase text-[10px] tracking-widest">Retry Status</th>
                                        <th class="border-b border-zinc-800 py-4 font-bold uppercase text-[10px] tracking-widest">Latest Exception</th>
                                        <th class="border-b border-zinc-800 py-4 font-bold uppercase text-[10px] tracking-widest">In Queue</th>
                                        <th class="border-b border-zinc-800 py-4 font-bold uppercase text-[10px] tracking-widest pr-8 text-right">Status</th>
                                    </tr>
                                </thead>
                                <tbody class="text-zinc-300">
                        """);

        if (response.getEvents() == null || response.getEvents().isEmpty()) {
            sb.append(
                    """
                            <tr>
                                <td colspan="6" class="py-32 text-center">
                                    <div class="flex flex-col items-center justify-center opacity-20">
                                        <h3 class="text-xl font-bold uppercase tracking-widest text-white mb-2">No Events</h3>
                                        <p class="text-xs font-medium max-w-xs mx-auto text-zinc-400">The dead letter queue is currently clear for topic '{{TOPIC}}'</p>
                                    </div>
                                </td>
                            </tr>
                            """
                            .replace("{{TOPIC}}", response.getTopic() != null ? response.getTopic() : "N/A"));
        } else {
            for (DLQEventSummary event : response.getEvents()) {
                String severityClass = switch (event.getSeverity()) {
                    case "HIGH" -> "bg-red-500/10 text-red-500 border-red-500/20";
                    case "MEDIUM" -> "bg-amber-500/10 text-amber-500 border-amber-500/20";
                    default -> "bg-zinc-800 text-zinc-400 border-zinc-700 font-medium";
                };

                sb.append("<tr class=\"hover:bg-zinc-800/20 transition-colors group\">");
                sb.append(
                        "<td class=\"border-b border-zinc-800/50 pl-8\"><div class=\"badge badge-outline border px-2 py-2 font-bold text-[9px] uppercase tracking-wider "
                                + severityClass + "\">" + event.getSeverity() + "</div></td>");
                sb.append("<td class=\"border-b border-zinc-800/50\">");
                sb.append("<div class=\"flex items-center gap-2\">");
                sb.append("<div class=\"font-bold text-white text-sm\">"
                        + (event.getEventId() != null ? event.getEventId() : "N/A") + "</div>");
                if (event.getEventId() != null) {
                    sb.append("<button onclick=\"copyToClipboard('" + event.getEventId()
                            + "', this)\" class=\"text-[9px] font-bold text-zinc-600 hover:text-white opacity-0 group-hover:opacity-100 transition-all uppercase\">Copy</button>");
                }
                sb.append("</div>");
                sb.append("<div class=\"text-[10px] font-bold text-zinc-500 uppercase tracking-tighter\">"
                        + (event.getEventType() != null ? event.getEventType() : "UNKNOWN") + "</div>");
                sb.append("</td>");
                sb.append("<td class=\"border-b border-zinc-800/50\">");

                int max = event.getMaxAttemptsConfigured();
                int current = event.getTotalAttempts();
                int percent = max > 0 ? (int) ((double) current / max * 100) : 0;

                sb.append("<div class=\"flex items-center gap-3\">");
                sb.append("<div class=\"w-12 bg-zinc-800 h-1 rounded-full overflow-hidden\">");
                sb.append("<div class=\"bg-white h-full transition-all duration-1000\" style=\"width: " + percent
                        + "%\"></div>");
                sb.append("</div>");
                sb.append("<span class=\"text-[10px] font-mono text-zinc-500\">" + current + "/" + max + "</span>");
                sb.append("</div>");
                sb.append("</td>");
                sb.append("<td class=\"max-w-md border-b border-zinc-800/50 py-4\">");
                if (event.getLastException() != null) {
                    sb.append("<div class=\"font-bold text-[11px] text-red-400 capitalize tracking-tight mb-1\">"
                            + event.getLastException().getType() + "</div>");
                    sb.append(
                            "<div class=\"text-xs font-medium text-zinc-400 leading-relaxed line-clamp-2 hover:line-clamp-none transition-all cursor-default\">"
                                    + event.getLastException().getMessage() + "</div>");
                } else {
                    sb.append("<span class=\"text-[11px] text-zinc-600\">No details</span>");
                }
                sb.append("</td>");
                sb.append(
                        "<td class=\"border-b border-zinc-800/50\"><div class=\"text-[11px] font-medium text-zinc-400\">"
                                + (event.getTimeInDlq() != null ? event.getTimeInDlq() : "N/A") + "</div></td>");
                sb.append("<td class=\"border-b border-zinc-800/50 pr-8 text-right\">");
                String statusClass = "FAILED_MAX_RETRIES".equals(event.getStatus()) ? "text-red-500" : "text-zinc-500";
                sb.append("<span class=\"text-[10px] font-bold uppercase tracking-widest " + statusClass + "\">"
                        + event.getStatus() + "</span>");
                sb.append("</td>");
                sb.append("</tr>");
            }
        }

        sb.append("""
                        </tbody>
                    </table>
                </div>
                """);
        return sb.toString();
    }
}
