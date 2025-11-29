#include "config.hpp"

#include "utils/cli_parser.hpp"

void Config::parse(int argc, char** argv) {
    cli::Parser parser(argc, argv);
    parser.parse("--setup_mode", setup_mode, cli::Parser::optional);
    parser.parse("--reg_ring", reg_ring, cli::Parser::optional);
    parser.parse("--reg_fds", reg_fds, cli::Parser::optional);
    parser.parse("--reg_bufs", reg_bufs, cli::Parser::optional);
    parser.parse("--iopoll", iopoll, cli::Parser::optional);
    parser.parse("--nvme_cmds", nvme_cmds, cli::Parser::optional);

    parser.parse("--core_id", core_id, cli::Parser::optional);
    parser.parse("--stats_interval", stats_interval, cli::Parser::optional);
    parser.parse("--duration", duration, cli::Parser::optional);


    parser.parse("--ssd", ssd);
    parser.parse("--virt_size", virt_size, cli::Parser::optional);
    parser.parse("--phys_size", phys_size, cli::Parser::optional);
    parser.parse("--concurrency", concurrency, cli::Parser::optional);
    parser.parse("--evict_batch", evict_batch, cli::Parser::optional);
    parser.parse("--free_target", free_target, cli::Parser::optional);
    parser.parse("--page_table_factor", page_table_factor, cli::Parser::optional);

    parser.parse("--workload", workload);
    parser.parse("--submit_always", submit_always, cli::Parser::optional);
    parser.parse("--sync_variant", sync_variant, cli::Parser::optional);
    parser.parse("--posix_variant", posix_variant, cli::Parser::optional);
    parser.parse("--ycsb_tuple_count", ycsb_tuple_count, cli::Parser::optional);
    parser.parse("--ycsb_read_ratio", ycsb_read_ratio, cli::Parser::optional);
    parser.parse("--tpcc_warehouses", tpcc_warehouses, cli::Parser::optional);

    parser.parse("--libaio", libaio, cli::Parser::optional);

    parser.check_unparsed();
    parser.print();

    ensure(ssd.size() > 0);

    if (nvme_cmds) {
        // /dev/ng1n1
        ensure(ssd.starts_with("/dev/ng"));
    }

    if (posix_variant) {
        ensure(sync_variant);
    }
}
