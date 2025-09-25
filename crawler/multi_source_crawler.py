import sys
import os
import json
import time
from datetime import datetime
from pathlib import Path

# Add the crawler directory to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from rsoe_crawler import RSOECrawler, merge_events, create_backup_if_needed, update_past_events_archive
from reliefweb_crawler import ReliefWebCrawler
from emsc_crawler import EMSCCrawler
from dynamic_geocoder import DynamicGeocoder

class MultiSourceCrawler:
    def __init__(self):
        self.rsoe_crawler = RSOECrawler()
        self.reliefweb_crawler = ReliefWebCrawler()
        self.emsc_crawler = EMSCCrawler()
        self.geocoder = DynamicGeocoder()
        self.all_events = []

    def crawl_all_sources(self):
        """Crawl all disaster data sources"""
        print("=" * 80)
        print("🌍 MULTI-SOURCE DISASTER DATA CRAWLER")
        print("🔄 Collecting data from RSOE, ReliefWeb, and EMSC")
        print("=" * 80)

        all_collected_events = []
        source_stats = {}

        # 1. RSOE Crawler
        print("\n" + "=" * 60)
        print("1️⃣  CRAWLING RSOE EDIS")
        print("=" * 60)
        try:
            rsoe_success = self.rsoe_crawler.crawl_events()

            if rsoe_success:
                rsoe_events = self.rsoe_crawler.collected_events or []
                all_collected_events.extend(rsoe_events)
                source_stats['RSOE'] = len(rsoe_events)
                print(f"✓ RSOE: Collected {len(rsoe_events)} events")
            else:
                source_stats['RSOE'] = 0
                print("⚠️ RSOE: No events collected")
        except Exception as e:
            print(f"❌ RSOE crawler failed: {e}")
            source_stats['RSOE'] = 0

        # 2. ReliefWeb Crawler
        print("\n" + "=" * 60)
        print("2️⃣  CRAWLING RELIEFWEB")
        print("=" * 60)
        try:
            reliefweb_success = self.reliefweb_crawler.crawl_disasters()

            if reliefweb_success:
                reliefweb_events = self.reliefweb_crawler.get_events()
                all_collected_events.extend(reliefweb_events)
                source_stats['ReliefWeb'] = len(reliefweb_events)
                print(f"✓ ReliefWeb: Collected {len(reliefweb_events)} events")
            else:
                source_stats['ReliefWeb'] = 0
                print("⚠️ ReliefWeb: No events collected")
        except Exception as e:
            print(f"❌ ReliefWeb crawler failed: {e}")
            source_stats['ReliefWeb'] = 0

        # 3. EMSC Crawler
        print("\n" + "=" * 60)
        print("3️⃣  CRAWLING EMSC EARTHQUAKES")
        print("=" * 60)
        try:
            emsc_success = self.emsc_crawler.crawl_earthquakes()

            if emsc_success:
                emsc_events = self.emsc_crawler.get_events()
                all_collected_events.extend(emsc_events)
                source_stats['EMSC'] = len(emsc_events)
                print(f"✓ EMSC: Collected {len(emsc_events)} earthquakes")
            else:
                source_stats['EMSC'] = 0
                print("⚠️ EMSC: No earthquakes collected")
        except Exception as e:
            print(f"❌ EMSC crawler failed: {e}")
            source_stats['EMSC'] = 0

        # Summary of collection
        print("\n" + "=" * 60)
        print("📊 COLLECTION SUMMARY")
        print("=" * 60)
        total_new_events = len(all_collected_events)
        print(f"Total new events collected: {total_new_events}")
        print("Events by source:")
        for source, count in source_stats.items():
            percentage = (count / total_new_events * 100) if total_new_events > 0 else 0
            print(f"  📡 {source}: {count} events ({percentage:.1f}%)")

        if total_new_events > 0:
            # Show category distribution
            category_stats = {}
            source_category_stats = {}

            for event in all_collected_events:
                category = event.get('event_category', 'Unknown')
                source = event.get('data_source', event.get('source', 'Unknown'))

                category_stats[category] = category_stats.get(category, 0) + 1

                if source not in source_category_stats:
                    source_category_stats[source] = {}
                source_category_stats[source][category] = source_category_stats[source].get(category, 0) + 1

            print(f"\nEvents by category (across all sources):")
            for category, count in sorted(category_stats.items()):
                percentage = count / total_new_events * 100
                print(f"  🏷️  {category}: {count} events ({percentage:.1f}%)")

            print(f"\nDetailed breakdown by source and category:")
            for source, categories in source_category_stats.items():
                print(f"  📡 {source}:")
                for category, count in sorted(categories.items()):
                    print(f"    🏷️  {category}: {count}")

        self.all_events = all_collected_events
        return len(all_collected_events) > 0

    def save_and_merge(self):
        """Save collected data and merge with existing events"""
        print("\n" + "=" * 60)
        print("4️⃣  MERGING AND SAVING DATA")
        print("=" * 60)

        try:
            # Create backup first
            print("📦 Creating backup of existing data...")
            create_backup_if_needed("docs/data/events.json")

            # Apply dynamic geocoding to events with missing coordinates
            print("🌍 Applying dynamic geocoding to events with missing coordinates...")
            geocoded_events = []
            geocoded_count = 0

            for event in self.all_events:
                # Check if coordinates are missing or zero
                def is_missing_coordinate(coord_str):
                    if not coord_str or coord_str in ['', '0', '0.0', '0.00']:
                        return True
                    try:
                        coord_val = float(coord_str)
                        return coord_val == 0.0
                    except:
                        return True

                current_lat = str(event.get('latitude', '')).strip()
                current_lon = str(event.get('longitude', '')).strip()

                if is_missing_coordinate(current_lat) or is_missing_coordinate(current_lon):
                    address = event.get('address', '')
                    if address and address != '-':
                        lat, lon = self.geocoder.get_coordinates(address, event)
                        if lat and lon:
                            event['latitude'] = lat
                            event['longitude'] = lon
                            geocoded_count += 1

                geocoded_events.append(event)

            if geocoded_count > 0:
                print(f"✅ Enhanced {geocoded_count} events with dynamic geocoding")

            # Merge with existing events using the enhanced merge function
            print("🔄 Merging new events with existing database...")
            merged_events = merge_events(
                geocoded_events,
                existing_path="docs/data/events.json",
                past_events_path="docs/data/past_events.json"
            )

            # Save merged results
            print("💾 Saving merged data...")
            output_file = "docs/data/events.json"
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(merged_events, f, ensure_ascii=False, indent=2)

            print(f"✅ Final merged events saved to {output_file}: {len(merged_events)} total events")

            # Update past events archive
            print("📚 Updating past events archive...")
            update_past_events_archive()

            # Final statistics
            if merged_events:
                print("\n" + "=" * 60)
                print("📈 FINAL DATABASE STATISTICS")
                print("=" * 60)

                source_breakdown = {}
                category_breakdown = {}

                for event in merged_events:
                    # Count by data source
                    source = event.get('data_source',
                            'rsoe' if not event.get('event_id', '').startswith(('RW_', 'EMSC_'))
                            else ('reliefweb' if event.get('event_id', '').startswith('RW_')
                                  else 'emsc'))
                    source_breakdown[source] = source_breakdown.get(source, 0) + 1

                    # Count by category
                    category = event.get('event_category', 'Unknown')
                    category_breakdown[category] = category_breakdown.get(category, 0) + 1

                print(f"📊 Total events in database: {len(merged_events)}")
                print("\n🏷️  Events by category:")
                for category, count in sorted(category_breakdown.items()):
                    percentage = count / len(merged_events) * 100
                    print(f"  {category}: {count} ({percentage:.1f}%)")

                print("\n📡 Events by data source:")
                for source, count in sorted(source_breakdown.items()):
                    percentage = count / len(merged_events) * 100
                    print(f"  {source.upper()}: {count} ({percentage:.1f}%)")

            return True

        except Exception as e:
            print(f"❌ Error during save and merge: {e}")
            import traceback
            traceback.print_exc()
            return False

    def run(self):
        """Run the complete multi-source crawling process"""
        start_time = datetime.now()

        try:
            # Crawl all sources
            success = self.crawl_all_sources()

            if not success:
                print("\n⚠️ No new events collected from any source")
                # Still try to run merge in case we need to archive old events
                self.all_events = []

            # Merge and save data
            merge_success = self.save_and_merge()

            # Final summary
            end_time = datetime.now()
            duration = end_time - start_time

            print("\n" + "=" * 80)
            print("🎯 MULTI-SOURCE CRAWLING COMPLETED")
            print("=" * 80)
            print(f"⏱️  Total execution time: {duration}")
            print(f"🔢 Total new events collected: {len(self.all_events)}")
            print(f"💾 Data merge and save: {'✅ Success' if merge_success else '❌ Failed'}")

            if success or merge_success:
                print("🌟 Process completed successfully!")
                return 0
            else:
                print("⚠️ Process completed with warnings")
                return 1

        except Exception as e:
            print(f"\n💥 FATAL ERROR: {e}")
            import traceback
            traceback.print_exc()
            return 1


def main():
    """Main entry point"""
    crawler = MultiSourceCrawler()
    return crawler.run()


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
