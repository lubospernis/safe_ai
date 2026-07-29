"use client";

import styles from "@/app/report/report.module.css";

interface DataRow {
  [key: string]: string | number | null;
}

interface Props {
  rows: DataRow[];
  valueCol: string;
  seriesCol: string;
  xCol?: string;
}

function asNumber(value: string | number | null | undefined): number | null {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

export default function SimpleLineChart({
  rows,
  valueCol,
  seriesCol,
  xCol = "wave_number",
}: Props) {
  const prepared = rows
    .map((r) => {
      const x = asNumber(r[xCol] as string | number | null | undefined);
      const y = asNumber(r[valueCol] as string | number | null | undefined);
      const s = String(r[seriesCol] ?? "");
      return { x, y, s };
    })
    .filter((r) => r.x !== null && r.y !== null && r.s);

  if (prepared.length === 0) {
    return <p className={styles.chartEmpty}>No chart data available.</p>;
  }

  const series = Array.from(new Set(prepared.map((r) => r.s)));
  const bySeries = series.map((s) => prepared.filter((r) => r.s === s).sort((a, b) => (a.x as number) - (b.x as number)));

  const xs = prepared.map((r) => r.x as number);
  const ys = prepared.map((r) => r.y as number);
  const minX = Math.min(...xs);
  const maxX = Math.max(...xs);
  const minY = Math.min(...ys);
  const maxY = Math.max(...ys);

  const width = 720;
  const height = 280;
  const padX = 42;
  const padY = 22;
  const plotW = width - padX * 2;
  const plotH = height - padY * 2;

  const xScale = (x: number) => {
    if (minX === maxX) return padX + plotW / 2;
    return padX + ((x - minX) / (maxX - minX)) * plotW;
  };
  const yScale = (y: number) => {
    if (minY === maxY) return padY + plotH / 2;
    return padY + (1 - (y - minY) / (maxY - minY)) * plotH;
  };

  const palette = ["#2B5291", "#0086DE", "#008C7A", "#A63559", "#FF7430"];

  return (
    <div className={styles.chartWrap}>
      <svg viewBox={`0 0 ${width} ${height}`} className={styles.chartSvg} role="img" aria-label="Section trend chart">
        <rect x="0" y="0" width={width} height={height} fill="#f8fafb" />
        <line x1={padX} y1={height - padY} x2={width - padX} y2={height - padY} stroke="#bfc8d4" strokeWidth="1" />
        <line x1={padX} y1={padY} x2={padX} y2={height - padY} stroke="#bfc8d4" strokeWidth="1" />
        {bySeries.map((lineRows, i) => {
          const points = lineRows.map((r) => `${xScale(r.x as number)},${yScale(r.y as number)}`).join(" ");
          const color = palette[i % palette.length];
          return (
            <g key={series[i]}>
              <polyline fill="none" stroke={color} strokeWidth="2.2" points={points} />
              {lineRows.map((r, idx) => (
                <circle key={`${series[i]}-${idx}`} cx={xScale(r.x as number)} cy={yScale(r.y as number)} r="2.7" fill={color} />
              ))}
            </g>
          );
        })}
      </svg>
      <div className={styles.chartLegend}>
        {series.map((s, i) => (
          <span key={s} className={styles.chartLegendItem}>
            <span className={styles.chartLegendDot} style={{ backgroundColor: palette[i % palette.length] }} />
            {s}
          </span>
        ))}
      </div>
    </div>
  );
}
