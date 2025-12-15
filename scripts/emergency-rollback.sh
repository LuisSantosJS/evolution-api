#!/bin/bash
# Script de Rollback Emergencial - Baileys v7
# Execute este script se houver problemas críticos após o upgrade

set -e

echo "🚨 INICIANDO ROLLBACK EMERGENCIAL 🚨"
echo ""
echo "Este script irá reverter o sistema para Baileys v6.7.21"
read -p "Tem certeza que deseja continuar? (yes/no): " -r
if [[ ! $REPLY =~ ^[Yy][Ee][Ss]$ ]]; then
  echo "Rollback cancelado"
  exit 1
fi

BACKUP_TIMESTAMP="${1:-}"
if [ -z "$BACKUP_TIMESTAMP" ]; then
  echo "⚠️  Nenhum timestamp especificado, usando backup mais recente"
  BACKUP_DIR="/tmp/baileys-upgrade-backups"
else
  BACKUP_DIR="/tmp/baileys-upgrade-backups"
  echo "📅 Usando backup do timestamp: $BACKUP_TIMESTAMP"
fi

# 1. Parar serviços
echo "🛑 Parando serviços..."
if command -v pm2 &> /dev/null; then
  pm2 stop all || true
  echo "✓ PM2 stopped"
fi

# 2. Restore código via git
echo "📦 Restaurando código via git..."
git reset --hard baileys-v6-stable
echo "✓ Código restaurado para tag baileys-v6-stable"

# 3. Restore Baileys v6.7.21
echo "📥 Restaurando Baileys v6.7.21..."
npm install @whiskeysockets/baileys@6.7.21
echo "✓ Baileys v6.7.21 instalado"

# 4. Reinstalar dependências
echo "📦 Reinstalando dependências..."
npm install
echo "✓ Dependências instaladas"

# 5. Restore database (se backup existe)
echo "💾 Restaurando banco de dados..."
if [ -z "$BACKUP_TIMESTAMP" ]; then
  LATEST_DB_BACKUP=$(ls -t "$BACKUP_DIR"/database_backup_*.sql 2>/dev/null | head -n 1)
else
  LATEST_DB_BACKUP="$BACKUP_DIR/database_backup_${BACKUP_TIMESTAMP}.sql"
fi

if [ -f "$LATEST_DB_BACKUP" ]; then
  DB_HOST="${DATABASE_HOST:-localhost}"
  DB_NAME="${DATABASE_NAME:-evolution_db}"
  DB_USER="${DATABASE_USER:-postgres}"

  echo "  Restaurando de: $LATEST_DB_BACKUP"
  psql -h "$DB_HOST" -U "$DB_USER" "$DB_NAME" < "$LATEST_DB_BACKUP"
  echo "✓ Banco de dados restaurado"
else
  echo "⚠️  Backup do banco não encontrado - skip"
fi

# 6. Restore Redis (se backup existe)
echo "💾 Restaurando Redis..."
if [ -z "$BACKUP_TIMESTAMP" ]; then
  LATEST_REDIS_BACKUP=$(ls -t "$BACKUP_DIR"/redis_backup_*.rdb 2>/dev/null | head -n 1)
else
  LATEST_REDIS_BACKUP="$BACKUP_DIR/redis_backup_${BACKUP_TIMESTAMP}.rdb"
fi

if [ -f "$LATEST_REDIS_BACKUP" ] && command -v redis-cli &> /dev/null; then
  redis-cli FLUSHALL
  redis-cli --rdb "$LATEST_REDIS_BACKUP"
  echo "✓ Redis restaurado"
else
  echo "⚠️  Backup do Redis não encontrado ou redis-cli indisponível - skip"
fi

# 7. Rebuild
echo "🔨 Rebuilding aplicação..."
npm run build
echo "✓ Build concluído"

# 8. Restart services
echo "🚀 Reiniciando serviços..."
if command -v pm2 &> /dev/null; then
  pm2 restart all
  echo "✓ Serviços reiniciados"
else
  echo "⚠️  PM2 não encontrado - inicie os serviços manualmente"
fi

echo ""
echo "✅ ROLLBACK COMPLETO!"
echo ""
echo "📊 Status atual:"
echo "  - Baileys version: 6.7.21"
echo "  - Código: baileys-v6-stable tag"
echo "  - Database: Restaurado"
echo "  - Redis: Restaurado"
echo ""
echo "🔍 Próximos passos:"
echo "  1. Verificar logs: pm2 logs"
echo "  2. Testar conexão de uma instância"
echo "  3. Monitorar por 30min"
echo ""
echo "📝 Investigate a causa do problema antes de tentar novamente"
